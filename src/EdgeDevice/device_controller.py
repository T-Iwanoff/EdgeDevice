import asyncio
from enum import Enum
from src.EdgeDevice.dashboard_connection import send_results
from src.EdgeDevice.file_manager import clear_old_files, has_files, MODEL_DIR, TRAINING_DIR, PROGRAM_DIR, DEVICE_PATH
import json
import importlib


class DeviceStatus(Enum):
    MISSING_DATA = "missing training data"
    MISSING_PROGRAM = "missing training program"
    IDLE = "idle"
    TRAINING = "training"


def load_device_info(name):
    if DEVICE_PATH.exists():
        return json.load(open(DEVICE_PATH))
    status = check_missing_files()
    return {"name": name, "id": None, "status": status.name}


def save_device_info(device_info):
    DEVICE_PATH.write_text(json.dumps(device_info, indent=2))


def check_missing_files():
    if not has_files(TRAINING_DIR):
        print("missing training data")
        return DeviceStatus.MISSING_DATA
    elif not has_files(PROGRAM_DIR):
        print("missing program")
        return DeviceStatus.MISSING_PROGRAM
    else:
        print("Found program and training data")
        return DeviceStatus.IDLE


def reload_trainer():
    try:
        from data.program import trainer
        importlib.reload(trainer)
        return trainer
    except ImportError:
        raise ImportError("Failed to load trainer")


class DeviceController:
    def __init__(self, ws):
        self.ws = ws
        self.device_info = None
        self.training_task = None

    async def initialize(self, device_name: "Unknown device"):
        self.device_info = load_device_info(device_name)
        state = check_missing_files()
        if state == DeviceStatus.IDLE and self.device_info["status"] == DeviceStatus.TRAINING.name:
            await self.start_training()  # crash recovery: resume training if interrupted
        else:
            self.device_info["status"] = state.value
            save_device_info(self.device_info)

    async def update_status(self):
        print("Updating status")
        state = check_missing_files()
        if state == DeviceStatus.IDLE:
            if self.training_task and not self.training_task.done():
                state = DeviceStatus.TRAINING
        await self.set_status(state)
        return state

    async def set_status(self, state):
        self.device_info["status"] = state.value
        save_device_info(self.device_info)
        await self._notify_state_change()

    def set_id(self, device_id):
        self.device_info["id"] = device_id
        save_device_info(self.device_info)

    async def _notify_state_change(self):
        try:
            await self.ws.send(json.dumps({  #TODO fix this (what needs fixing?)
                "type": "update_status",
                "payload": {
                    "name": self.device_info["name"],
                    "id": self.device_info["id"],
                    "status": self.device_info["status"]
                }
            }))
        except Exception as e:
            print("Failed to notify server:", e)

    async def handle_train_command(self):
        if not self.device_info["status"] == DeviceStatus.IDLE.value:
            return  #TODO return why it wont start training (tell server its current status?)
        await self.start_training()

    async def start_training(self):
        if self.training_task and not self.training_task.done():
            return  # already running
        await self.set_status(DeviceStatus.TRAINING)
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

            await self.set_status(DeviceStatus.IDLE)

        except Exception as e:
            print("Training crashed:", e)

            # IMPORTANT: leave state as TRAINING for restart recovery
            await self.set_status(DeviceStatus.TRAINING)
            raise
