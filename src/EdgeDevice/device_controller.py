import asyncio
from enum import Enum
from state_manager import has_files, MODEL_DIR, TRAINING_DIR


class DeviceState(Enum):
    MISSING_DATA = "missing training data"
    IDLE = "idle"
    TRAINING = "training"


class DeviceController:
    def __init__(self, ws, config):
        self.ws = ws
        self.config = config
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
            if self.config.get("status") == "training":
                await self.start_training()
            else:
                await self.set_state(DeviceState.IDLE)

    # ----------------------------
    # state transitions
    # ----------------------------
    async def set_state(self, state):
        self.state = state
        self.config["status"] = state.value
        await self._notify_state_change()

    async def _notify_state_change(self):
        try:
            await self.ws.send_json({
                "type": "update_status",
                "payload": {
                    "status": self.config["status"]
                }
            })
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

            # dummy training
            await asyncio.sleep(20)

            print("Training finished")

            # notify server
            await self.ws.send_json({
                "type": "training_complete",
                "payload": self.config
            })

            await self.set_state(DeviceState.IDLE)

        except Exception as e:
            print("Training crashed:", e)

            # IMPORTANT: leave state as TRAINING for restart recovery
            await self.set_state(DeviceState.TRAINING)
            raise