import asyncio
from enum import Enum
from data.program import trainer
from src.EdgeDevice.dashboard_connection import send_results
from src.EdgeDevice.file_manager import clear_old_files
from state_manager import has_files, MODEL_DIR, TRAINING_DIR
import json
import importlib


class DeviceState(Enum):
    MISSING_DATA = "missing training data"
    IDLE = "idle"
    TRAINING = "training"

def reload_trainer():
    importlib.reload(trainer)
    return trainer

class DeviceController:
    def __init__(self, ws, device_info):
        self.ws = ws
        self.device_info = device_info
        self.state = None
        self.training_task = None

    # ----------------------------
    # startup recovery
    # ----------------------------
    async def initialize(self):
        if not has_files(TRAINING_DIR) or not has_files(MODEL_DIR):
            await self.set_state(DeviceState.MISSING_DATA)
        else:
            # crash recovery: resume training if interrupted
            if self.device_info.get("status") == "training":
                await self.start_training()
            else:
                await self.set_state(DeviceState.IDLE)

    # ----------------------------
    # state transitions
    # ----------------------------
    async def set_state(self, state):
        self.state = state
        self.device_info["status"] = state.value
        await self._notify_state_change()

    async def _notify_state_change(self):
        try:
            await self.ws.send(json.dumps({ #TODO fix this
                "type": "update_status",
                "payload": {
                    "name": self.device_info["name"],
                    "id": self.device_info["id"],
                    "status": self.device_info["status"]
                }
            }))
        except Exception as e:
            print("Failed to notify server:", e)

    # ----------------------------
    # server command
    # ----------------------------
    async def handle_train_command(self):
        if self.state == DeviceState.MISSING_DATA:
            return

        await self.start_training()

    # ----------------------------
    # training lifecycle
    # ----------------------------
    async def start_training(self):
        if self.training_task and not self.training_task.done():
            return  # already running

        await self.set_state(DeviceState.TRAINING)

        self.training_task = asyncio.create_task(self._train())

    async def _train(self):
        try:
            print("Training started...")
            module = reload_trainer()  # pick up latest version
            result = await module.main(TRAINING_DIR, MODEL_DIR)
            await clear_old_files(MODEL_DIR, result)
            print("Training finished")

            await send_results(self.device_info["id"])
            await self.ws.send(json.dumps({
                "type": "training_complete",
                "payload": {
                    "name": self.device_info["name"],
                    "id": self.device_info["id"],
                    "status": self.device_info["status"]
                }
            }))

            await self.set_state(DeviceState.IDLE)

        except Exception as e:
            print("Training crashed:", e)

            # IMPORTANT: leave state as TRAINING for restart recovery
            await self.set_state(DeviceState.TRAINING)
            raise