#!/usr/bin/env python3
"""
opc_ua.py
---------
Layer 3: Interface Projection Layer (OPC UA Target implementation).
Exposes the internal Stateful Canonical Model via an object-oriented, typed, 
and hierarchical OPC UA Information Model address space.
"""

import asyncio
import logging
from typing import Dict, Any, Optional
from asyncua import Server, ua

from models import CanonicalState

logger = logging.getLogger("LithoAdapter.OpcUa")

class OpcUaServerProjection:
    """
    Layer 3 Adapter projection mapping the Canonical State Model to a strict 
    hierarchical OPC UA address space structure.
    """
    def __init__(self, target_state: CanonicalState, endpoint: str = "opc.tcp://0.0.0.0:4840/freeopcua/server/", name: str = "LithoStepper_LS200"):
        self.state = target_state
        self.endpoint = endpoint
        self.name = name
        
        self.server = Server()
        self.idx: int = 0
        
        # Dictionary caching Node variables for rapid high-frequency streaming mutations
        self.nodes: Dict[str, Any] = {}
        self._is_running = False

    async def init_server(self):
        """Initializes the endpoint bindings, namespaces, and structured Address Space topology."""
        await self.server.init()
        self.server.set_endpoint(self.endpoint)
        self.server.set_server_name(self.name)
        
        # Establish isolated custom manufacturing namespace
        self.idx = await self.server.register_namespace("http://qpiai.tech/lithography/")
        
        # 1. Instantiate Core Root Equipment Object Node
        root_objects = self.server.nodes.objects
        tool_obj = await root_objects.add_object(self.idx, self.name)
        
        # 2. Structure Component Hierarchy Blocks
        eq_folder = await tool_obj.add_folder(self.idx, "EquipmentStatus")
        prod_folder = await tool_obj.add_folder(self.idx, "ProductionContext")
        telem_folder = await tool_obj.add_folder(self.idx, "Telemetry")
        alarms_folder = await tool_obj.add_folder(self.idx, "Alarms")
        metrics_folder = await tool_obj.add_folder(self.idx, "AdapterMetrics")

        # 3. Define Typed Variable Nodes beneath respective Structural Parents
        
        # --- Equipment Status Variables ---
        self.nodes["current_state"] = await eq_folder.add_variable(self.idx, "CurrentState", "NON_SCHEDULED", ua.VariantType.String)
        self.nodes["previous_state"] = await eq_folder.add_variable(self.idx, "PreviousState", "NON_SCHEDULED", ua.VariantType.String)
        self.nodes["last_state_change"] = await eq_folder.add_variable(self.idx, "LastStateChange", "", ua.VariantType.String)
        
        # --- Production Context Variables ---
        self.nodes["current_lot"] = await prod_folder.add_variable(self.idx, "CurrentLot", "NONE", ua.VariantType.String)
        self.nodes["current_wafer"] = await prod_folder.add_variable(self.idx, "CurrentWafer", "NONE", ua.VariantType.String)
        self.nodes["current_slot"] = await prod_folder.add_variable(self.idx, "CurrentSlot", "NONE", ua.VariantType.String)
        self.nodes["recipe"] = await prod_folder.add_variable(self.idx, "Recipe", "NONE", ua.VariantType.String)
        self.nodes["reticle"] = await prod_folder.add_variable(self.idx, "Reticle", "NONE", ua.VariantType.String)
        self.nodes["expected_wafers"] = await prod_folder.add_variable(self.idx, "ExpectedWafers", 0, ua.VariantType.Int32)
        self.nodes["last_align_residual_nm"] = await prod_folder.add_variable(self.idx, "LastAlignResidual_nm", 0.0, ua.VariantType.Float)
        self.nodes["last_fields_count"] = await prod_folder.add_variable(self.idx, "LastFieldsCount", 0, ua.VariantType.Int32)
        self.nodes["last_dose_mj_cm2"] = await prod_folder.add_variable(self.idx, "LastDose_mj_cm2", 0.0, ua.VariantType.Float)

        # --- Active Telemetry Variables (Matching exact required physical types) ---
        self.nodes["stage_temp_c"] = await telem_folder.add_variable(self.idx, "StageTemp_C", 0.0, ua.VariantType.Float)
        self.nodes["chuck_vac_kpa"] = await telem_folder.add_variable(self.idx, "ChuckVac_kPa", 0.0, ua.VariantType.Float)
        self.nodes["laser_mj"] = await telem_folder.add_variable(self.idx, "Laser_mJ", 0.0, ua.VariantType.Float)
        self.nodes["illum_pct"] = await telem_folder.add_variable(self.idx, "Illum_Pct", 0.0, ua.VariantType.Float)
        self.nodes["focus_nm"] = await telem_folder.add_variable(self.idx, "Focus_nm", 0.0, ua.VariantType.Float)

        # --- Active Alarms Stream (Serialized as an array string due to dynamic set gaps) ---
        self.nodes["active_alarms"] = await alarms_folder.add_variable(self.idx, "ActiveAlarmsList", "[]", ua.VariantType.String)

        # --- Adapter Pipeline Metrics Variables ---
        self.nodes["total_processed_lines"] = await metrics_folder.add_variable(self.idx, "TotalProcessedLines", 0, ua.VariantType.Int64)
        self.nodes["malformed_lines_count"] = await metrics_folder.add_variable(self.idx, "MalformedLinesCount", 0, ua.VariantType.Int64)
        self.nodes["last_updated_at"] = await metrics_folder.add_variable(self.idx, "LastUpdatedAt", "", ua.VariantType.String)

        # Standard safety check: lock variable states to make them strictly Read-Only for MES industrial clients
        for node in self.nodes.values():
            await node.set_writable(False)

        logger.info(f"OPC UA Server Address space structured successfully on endpoint: {self.endpoint}")

    async def update_projection(self):
        """
        Reads data from the Canonical Model object structure, performs typing validation,
        and pushes writes directly into the local OPC UA nodes live memory buffer.
        """
        if not self.nodes:
            return

        try:
            # Update Equipment E10 Nodes
            await self.nodes["current_state"].write_value(str(self.state.current_state))
            await self.nodes["previous_state"].write_value(str(self.state.previous_state))
            await self.nodes["last_state_change"].write_value(
                self.state.state_changed_at.isoformat() if self.state.state_changed_at else ""
            )

            # Update Production Context Nodes (handling null checks safely for standard primitive variants)
            await self.nodes["current_lot"].write_value(str(self.state.current_lot or "NONE"))
            await self.nodes["current_wafer"].write_value(str(self.state.current_wafer or "NONE"))
            await self.nodes["current_slot"].write_value(str(self.state.current_slot or "NONE"))
            await self.nodes["recipe"].write_value(str(self.state.recipe or "NONE"))
            await self.nodes["reticle"].write_value(str(self.state.reticle or "NONE"))
            await self.nodes["expected_wafers"].write_value(int(self.state.expected_wafers_in_lot or 0), ua.VariantType.Int32)
            await self.nodes["last_align_residual_nm"].write_value(float(self.state.latest_alignment_residual_nm or 0.0), ua.VariantType.Float)
            await self.nodes["last_fields_count"].write_value(int(self.state.latest_exposure_fields or 0), ua.VariantType.Int32)
            await self.nodes["last_dose_mj_cm2"].write_value(float(self.state.latest_exposure_dose_mj_cm2 or 0.0), ua.VariantType.Float)

            # Update Telemetry Group Nodes
            tel = self.state.telemetry
            await self.nodes["stage_temp_c"].write_value(float(tel.get("stage_temp_c", 0.0)), ua.VariantType.Float)
            await self.nodes["chuck_vac_kpa"].write_value(float(tel.get("chuck_vac_kpa", 0.0)), ua.VariantType.Float)
            await self.nodes["laser_mj"].write_value(float(tel.get("laser_mj", 0.0)), ua.VariantType.Float)
            await self.nodes["illum_pct"].write_value(float(tel.get("illum_pct", 0.0)), ua.VariantType.Float)
            await self.nodes["focus_nm"].write_value(float(tel.get("focus_nm", 0.0)), ua.VariantType.Float)

            # Map the complex dynamic alarms map into a robust queryable string block snapshot
            import json
            alarms_json = json.dumps([
                {"alid": alid, "severity": info["severity"], "text": info["text"], "set_at": info["set_at"].isoformat()}
                for alid, info in self.state.active_alarms.items()
            ])
            await self.nodes["active_alarms"].write_value(alarms_json)

            # Update Pipeline Performance Nodes
            await self.nodes["total_processed_lines"].write_value(int(self.state.total_processed_lines), ua.VariantType.Int64)
            await self.nodes["malformed_lines_count"].write_value(int(self.state.malformed_lines_count), ua.VariantType.Int64)
            await self.nodes["last_updated_at"].write_value(
                self.state.last_updated_at.isoformat() if self.state.last_updated_at else ""
            )

        except Exception as err:
            logger.error(f"Failed to marshaling data down into OPC UA target node structures: {err}")

    async def start(self):
        """Starts the server runtime context execution loop."""
        await self.init_server()
        self._is_running = True
        await self.server.start()
        logger.info("OPC UA In-Process Daemon Server running live.")

    async def stop(self):
        """Gracefully terminates the network socket stack bindings."""
        self._is_running = False
        await self.server.stop()
        logger.info("OPC UA Daemon Server shut down cleanly.")