#!/usr/bin/env python3
"""
Synthetic log generator for a simplified photolithography exposure tool
("LithoStepper LS-200"), a fictional research-fab stepper that has NO
SECS/GEM or OPC UA interface and only writes flat text logs.

Deterministic for a given --seed. Use --drift to inject a labelled focus
drift for the SPC bonus task (see README for the documented ground truth).

Line format (pipe-delimited; payload is order-independent key=value pairs):

    <ISO8601_UTC>|<TYPE>|key=value|key=value|...

TYPEs: STATE (E10 equipment states), EVENT (lot/wafer lifecycle),
       TELEM (periodic sensor telemetry), ALARM (set / clear).

Deliberate, graded "real world" wrinkles (always present):
    1. Four line types interleaved at different rates.
    2. A lot (LOT4472) that spans the file-rotation boundary part1 -> part2.
    3. An alarm SET and CLEAR that are far apart (active-alarm tracking).
    4. A torn/truncated final line in part1 (simulated crash mid-write).
    5. A reordered-key TELEM line and a missing-key TELEM line.
    6. A stray non-conforming line (logrotate banner).

With --drift: focus_nm drifts upward across LOT4472 toward the spec limit
(FOCUS_SPEC_NM). An EWMA chart should flag the small shift before any single
raw reading breaches spec. Used by the SPC bonus.

Run (from the repo root):
    python tools/generate_logs.py                                   # core dataset -> data/
    python tools/generate_logs.py --drift --prefix data/litho_eqp_drift   # SPC dataset
"""

import argparse
import os
import random
from datetime import datetime, timedelta, timezone

RECIPES = ["POLY_193I_A", "VIA_193I_B", "METAL1_248_C"]
RETICLES = ["RET-8842", "RET-1190", "RET-5503"]

FOCUS_SPEC_NM = 15.0      # |focus_nm| > 15 is out of spec
DRIFT_STEP_NM = 0.95      # focus mean increase per exposure sample under --drift


def iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def kv(ts: datetime, typ: str, **fields) -> str:
    return "|".join([iso(ts), typ] + [f"{k}={v}" for k, v in fields.items()])


class Sim:
    def __init__(self, seed: int, drift: bool):
        self.rng = random.Random(seed)
        self.t = datetime(2026, 5, 20, 8, 0, 0, tzinfo=timezone.utc)
        self.lines = []
        self.state = "NON_SCHEDULED"
        self.drift = drift
        self.focus_bias = 0.0     # accumulates only while ramping under --drift

    def advance(self, lo, hi):
        self.t += timedelta(seconds=self.rng.uniform(lo, hi))

    def emit(self, line):
        self.lines.append(line)

    def telem(self, temp_bias=0.0, glitch="", ramp=False):
        if ramp and self.drift:
            self.focus_bias += DRIFT_STEP_NM
        temp = round(22.4 + self.rng.uniform(-0.15, 0.15) + temp_bias, 2)
        vac = round(88.0 + self.rng.uniform(-0.8, 0.8), 1)
        laser = round(15.0 + self.rng.uniform(-0.2, 0.2), 2)
        illum = round(99.6 + self.rng.uniform(-0.4, 0.3), 1)
        focus = int(round(self.rng.gauss(0, 2.0) + self.focus_bias))
        if glitch == "reorder":
            self.emit(kv(self.t, "TELEM", laser_mj=laser, focus_nm=focus,
                         stage_temp_c=temp, illum_pct=illum, chuck_vac_kpa=vac))
        elif glitch == "missing":   # focus_nm absent on purpose
            self.emit(kv(self.t, "TELEM", stage_temp_c=temp, chuck_vac_kpa=vac,
                         laser_mj=laser, illum_pct=illum))
        else:
            self.emit(kv(self.t, "TELEM", stage_temp_c=temp, chuck_vac_kpa=vac,
                         laser_mj=laser, illum_pct=illum, focus_nm=focus))

    def set_state(self, new):
        self.emit(kv(self.t, "STATE", equipment_state=new, prev=self.state))
        self.state = new

    def run_wafer(self, lot, wafer, slot, temp_alarm=False, force_glitch="", ramp=False):
        self.advance(0.5, 1.5)
        self.emit(kv(self.t, "EVENT", evt="WAFER_START", lot=lot, wafer=f"{wafer:02d}", slot=slot))
        self.advance(0.3, 0.8); self.telem()
        self.advance(1.0, 2.0)
        resid = round(self.rng.uniform(1.5, 4.5), 1)
        self.emit(kv(self.t, "EVENT", evt="ALIGN_COMPLETE", lot=lot, wafer=f"{wafer:02d}", align_resid_nm=resid))
        for i in range(self.rng.randint(3, 5)):     # exposure telemetry burst
            self.advance(0.4, 0.9)
            glitch = force_glitch if i == 1 else ""
            self.telem(temp_bias=0.4 if temp_alarm else 0.0, glitch=glitch, ramp=ramp)
        self.advance(0.5, 1.0)
        fields = self.rng.choice([76, 84, 88, 92])
        dose = round(32.0 + self.rng.uniform(-0.6, 0.6), 1)
        self.emit(kv(self.t, "EVENT", evt="EXPOSE_COMPLETE", lot=lot,
                     wafer=f"{wafer:02d}", fields=fields, dose_mj_cm2=dose))
        self.advance(0.4, 0.9)
        self.emit(kv(self.t, "EVENT", evt="WAFER_COMPLETE", lot=lot, wafer=f"{wafer:02d}"))

    def run_lot(self, lot, n_wafers, alarm_on_wafer=None, glitch_on_wafer=None, ramp=False):
        recipe = self.rng.choice(RECIPES)
        reticle = self.rng.choice(RETICLES)
        self.advance(2.0, 5.0)
        if self.state != "PRODUCTIVE":
            self.set_state("PRODUCTIVE")
        self.advance(0.5, 1.0)
        self.emit(kv(self.t, "EVENT", evt="LOT_START", lot=lot, recipe=recipe,
                     wafers=n_wafers, reticle=reticle))
        for w in range(1, n_wafers + 1):
            g = glitch_on_wafer.get(w, "") if glitch_on_wafer else ""
            self.run_wafer(lot, w, slot=w, temp_alarm=(w == alarm_on_wafer),
                           force_glitch=g, ramp=ramp)
            if w == alarm_on_wafer:
                self.advance(0.2, 0.5)
                self.emit(kv(self.t, "ALARM", alid=2107, state="SET",
                             sev="MAJOR", text="Focus offset out of spec"))
                self.advance(0.1, 0.3); self.set_state("UNSCHEDULED")
                self.advance(40, 70)
                self.emit(kv(self.t, "ALARM", alid=2107, state="CLEAR"))
                self.advance(0.2, 0.5); self.set_state("PRODUCTIVE")
        self.advance(1.0, 2.0)
        self.emit(kv(self.t, "EVENT", evt="LOT_COMPLETE", lot=lot, wafers_done=n_wafers))


def build(seed, drift):
    s = Sim(seed, drift)
    s.set_state("STANDBY"); s.advance(3, 6); s.telem()

    # Lot 1: completes in part1; focus alarm mid-lot; reordered-key TELEM on wafer 2.
    s.run_lot("LOT4471", 4, alarm_on_wafer=3, glitch_on_wafer={2: "reorder"})

    s.advance(8, 15); s.set_state("STANDBY"); s.advance(3, 6); s.telem()

    # Lot 2 (LOT4472): STARTS in part1, CONTINUES in part2 (spans rotation).
    # Under --drift, focus ramps across this lot.
    recipe = s.rng.choice(RECIPES); reticle = s.rng.choice(RETICLES)
    s.advance(2, 4); s.set_state("PRODUCTIVE"); s.advance(0.5, 1.0)
    s.emit(kv(s.t, "EVENT", evt="LOT_START", lot="LOT4472", recipe=recipe,
              wafers=5, reticle=reticle))
    s.run_wafer("LOT4472", 1, 1, ramp=drift)
    s.run_wafer("LOT4472", 2, 2, ramp=drift)

    # ---- ROTATION POINT ----
    split = len(s.lines)
    truncated = iso(s.t)[:19] + "|EVENT|evt=WAFER_STAR"     # torn mid-write
    part1 = s.lines[:split] + [truncated]

    s.advance(0.5, 1.5)
    s.run_wafer("LOT4472", 3, 3, ramp=drift)
    s.run_wafer("LOT4472", 4, 4, force_glitch="missing", ramp=drift)
    s.run_wafer("LOT4472", 5, 5, ramp=drift)
    s.advance(1.0, 2.0)
    s.emit(kv(s.t, "EVENT", evt="LOT_COMPLETE", lot="LOT4472", wafers_done=5))

    s.advance(1, 2); s.emit("==== logrotate: reopened LS-200.log ====")
    s.advance(2, 4); s.set_state("SCHEDULED"); s.advance(3, 5); s.telem()

    return part1, s.lines[split:]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--prefix", default="data/litho_eqp")
    ap.add_argument("--drift", action="store_true",
                    help="inject focus drift across LOT4472 for the SPC bonus")
    args = ap.parse_args()
    out_dir = os.path.dirname(args.prefix)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    part1, part2 = build(args.seed, args.drift)
    for name, lines in [(f"{args.prefix}_part1.log", part1), (f"{args.prefix}_part2.log", part2)]:
        with open(name, "w") as f:
            f.write("\n".join(lines) + "\n")
    tag = " (focus drift injected)" if args.drift else ""
    print(f"wrote {args.prefix}_part1.log ({len(part1)} lines), "
          f"{args.prefix}_part2.log ({len(part2)} lines)  seed={args.seed}{tag}")


if __name__ == "__main__":
    main()
