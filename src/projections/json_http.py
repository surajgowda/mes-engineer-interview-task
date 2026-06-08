#!/usr/bin/env python3
"""
json_http.py
------------
Layer 3: Interface Projection Layer.
Exposes the internal CanonicalState via a structured, hierarchical, and typed
JSON schema using FastAPI over standard HTTP REST and live WebSocket pushes.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from models import CanonicalState

# Configure layer-specific log stream
logger = logging.getLogger("LithoAdapter.Projection")

app = FastAPI(
    title="LithoStepper LS-200 Standard API Adapter",
    description="MES-compliant hierarchical Information Model interface for research-fab lithography tools.",
    version="1.0.0"
)

# Enable open cross-origin operations to ensure factory dashboards can monitor it natively
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global memory pointer to the state machine's state instance
_shared_canonical_state: Optional[CanonicalState] = None

# ==========================================
# STRICT HIERARCHICAL PYDANTIC SCHEMAS
# ==========================================

class EquipmentStateSchema(BaseModel):
    current_state: str = Field(..., description="SEMI E10 equipment operational state.")
    previous_state: str = Field(..., description="The immediate preceding SEMI E10 state.")
    last_state_change: Optional[str] = Field(None, description="ISO8601 UTC timestamp of last transition.")

class ProductionContextSchema(BaseModel):
    current_lot: Optional[str] = Field(None, description="Active Lot tracking identifier.")
    current_wafer: Optional[str] = Field(None, description="Active Wafer tracking ID (slot index format).")
    current_slot: Optional[str] = Field(None, description="Physical slot position index within cassette.")
    recipe: Optional[str] = Field(None, description="Active lithography exposure recipe program.")
    reticle: Optional[str] = Field(None, description="Active physical photomask/reticle ID.")
    expected_wafers: Optional[int] = Field(None, description="Total planned wafers inside the active lot batch.")
    last_align_residual_nm: Optional[float] = Field(None, description="Residual registration error from alignment step (nm).")
    last_fields_count: Optional[int] = Field(None, description="Total exposed shot fields on last processed wafer.")
    last_dose_mj_cm2: Optional[float] = Field(None, description="Exposure energy target density applied (mJ/cm²).")

class ActiveAlarmSchema(BaseModel):
    alid: str = Field(..., description="Unique Factory Alarm Identifier.")
    severity: str = Field(..., description="Fault impact severity classification (e.g. MAJOR, MINOR).")
    text: str = Field(..., description="Descriptive literal tracking message.")
    set_at: str = Field(..., description="ISO8601 UTC timestamp when the alarm triggered.")

class TelemetrySchema(BaseModel):
    stage_temp_c: float = Field(..., description="Sub-micron positioner stage thermal loop reading (°C).")
    chuck_vac_kpa: float = Field(..., description="Wafer flat holding vacuum loop pressure suction (kPa).")
    laser_mj: float = Field(..., description="Excimer laser source discharge discharge energy per pulse (mJ).")
    illum_pct: float = Field(..., description="Optical slit uniformity intensity percentage.")
    focus_nm: float = Field(..., description="Z-axis precision focus plane positioning displacement (nm).")

class AdapterMetricsSchema(BaseModel):
    total_processed_lines: int = Field(..., description="Total raw file log rows parsed since runtime start.")
    malformed_lines_count: int = Field(..., description="Total invalid lines dropped based on Malformed Policy.")
    last_updated_at: Optional[str] = Field(None, description="ISO8601 UTC timestamp of last incoming record ingestion.")

class CanonicalInformationModelSchema(BaseModel):
    """The master root model structure mapping directly to the MES standard hierarchy layout."""
    equipment: EquipmentStateSchema
    production_context: ProductionContextSchema
    active_alarms: List[ActiveAlarmSchema]
    telemetry: TelemetrySchema
    adapter_metrics: AdapterMetricsSchema


# ==========================================
# CLASS INTERFACE WRAPPER
# ==========================================

class HttpNotificationProjection:
    """Layer 3 Adapter projection management coordinator."""
    def __init__(self, target_state: CanonicalState):
        global _shared_canonical_state
        _shared_canonical_state = target_state
        self.connected_sockets: List[WebSocket] = []

    async def broadcast_state_change(self):
        """Pushes the current state out live to all streaming WebSocket subscribers."""
        if not self.connected_sockets or not _shared_canonical_state:
            return
            
        payload = _shared_canonical_state.to_dict()
        disconnected = []
        
        for ws in self.connected_sockets:
            try:
                await ws.send_json(payload)
            except Exception:
                disconnected.append(ws)
                
        for ws in disconnected:
            self.connected_sockets.remove(ws)


# ==========================================
# FASTAPI APP ROUTING ENDPOINTS
# ==========================================

@app.get(
    "/state", 
    response_model=CanonicalInformationModelSchema,
    summary="Fetch Current Equipment Snapshot",
    description="Returns a strictly validated, hierarchical snapshot of the tools current state."
)
def get_current_state():
    if not _shared_canonical_state:
        return {
            "equipment": {"current_state": "UNKNOWN", "previous_state": "UNKNOWN"},
            "production_context": {},
            "active_alarms": [],
            "telemetry": {"stage_temp_c": 0, "chuck_vac_kpa": 0, "laser_mj": 0, "illum_pct": 0, "focus_nm": 0},
            "adapter_metrics": {"total_processed_lines": 0, "malformed_lines_count": 0}
        }
    return _shared_canonical_state.to_dict()


@app.websocket("/stream")
async def websocket_stream_endpoint(websocket: WebSocket):
    """Live WebSocket pub-sub junction matching real-time MES event consumption designs."""
    if HttpNotificationProjection_ref := HttpNotificationProjection_global_pointer():
        await websocket.accept()
        HttpNotificationProjection_ref.connected_sockets.append(websocket)
        logger.info(f"New MES subscriber coupled to socket stream. Active feeds: {len(HttpNotificationProjection_ref.connected_sockets)}")
        
        try:
            # Send initial state snapshot immediately upon connection handshake
            if _shared_canonical_state:
                await websocket.send_json(_shared_canonical_state.to_dict())
                
            # Keep connection open loop listening for client heartbeat or disconnection
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            HttpNotificationProjection_ref.connected_sockets.remove(websocket)
            logger.info("MES Subscriber dropped socket connection feed.")
        except Exception as err:
            if websocket in HttpNotificationProjection_ref.connected_sockets:
                HttpNotificationProjection_ref.connected_sockets.remove(websocket)
            logger.debug(f"Socket connection terminated unexpectedly: {err}")


def HttpNotificationProjection_global_pointer() -> Optional[HttpNotificationProjection]:
    """Helper method to access active projection context inside standard functional routes safely."""
    if _shared_canonical_state is not None:
        # Re-instantiate the wrapper cleanly to pass the active pointer reference
        return HttpNotificationProjection(_shared_canonical_state)
    return None