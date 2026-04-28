import asyncio
import websockets
import json
import aiohttp
from pathlib import Path
from file_manager import clear_old_files
import httpx
import aiofiles
import yaml

CONFIG_PATH = Path("config.yaml")
DEVICE_PATH = Path("device.json")
DATA_DIR = Path(__file__).resolve().parents[2] / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def load_config():
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r") as f:
            return yaml.safe_load(f)
    raise FileNotFoundError("config.yaml not found")

yaml_config = load_config()
IP = yaml_config["ip"]
PORT = yaml_config["port"]

URL = f"{IP}:{PORT}"
URI = f"ws://{URL}/ws"


def load_device_info():
    if DEVICE_PATH.exists():
        return json.load(open(DEVICE_PATH))
    name = yaml_config["name"]
    return {"name": name, "id": None, "status": "idle"}


def save_device_info(device_info):
    DEVICE_PATH.write_text(json.dumps(device_info, indent=2))


async def download_file(url, file_id, file_type, ws, device_info):
    path = DATA_DIR / file_type
    path.mkdir(parents=True, exist_ok=True)
    file_path = path / file_id
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                with open(file_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(1024 * 1024):
                        f.write(chunk)
        print(f"Download complete: {file_path}")
        await clear_old_files(path, file_id)
        # notify server
        await ws.send(json.dumps({
            "type": "download_complete",
            "payload": {
                "file_id": file_id,
                "type": file_type,
                "id": device_info["id"]
            }
        }))
    except Exception as e:
        print("Download failed:", e)


async def send_results(device_id: int):
    url = f"http://{URL}/upload/results/{device_id}"
    path = DATA_DIR / "model" / "model.pth" # TODO get real path

    async with aiofiles.open(path, "rb") as f:
        content = await f.read()
    files = {"file": (path.name, content)}
    async with httpx.AsyncClient() as client:
        response = await client.post(url, files=files)

    print(f"response: {response.status_code}")


# Handles ALL incoming messages from server
async def receiver(ws, device_info, controller):
    while True:
        try:
            raw = await ws.recv()
            msg = json.loads(raw)
            print("Received:", msg)
            msg_type = msg.get("type")

            if msg_type == "get_status":
                await ws.send(json.dumps({
                    "type": "update_status",
                    "payload": device_info
                }))

            elif msg_type == "download_file":
                payload = msg.get("payload", {})
                url = payload.get("url")
                file_id = payload.get("file_id")
                file_type = payload.get("type") #TODO get("file_type", "training_data")?
                if url and file_id:
                    asyncio.create_task(download_file(url, file_id, file_type, ws, device_info))
                else:
                    print("Invalid download_file message")

            elif msg_type == "train":
                await controller.handle_train_command()

            elif msg_type == "ack":
                # Not doing anything here for now
                pass

            elif msg_type == "error":
                print("Server error:", msg.get("payload"))

            elif msg_type == "wrong_id":
                # TODO the client needs to register again
                pass

            else:
                print("Unknown message type:", msg_type)

        except Exception as e:
            print("Receiver error:", e)
            break


# Sends heartbeat periodically
async def heartbeat(ws, device_info):
    while True:
        try:
            if not device_info.get("id"):
                print("No device ID, skipping heartbeat")
                await asyncio.sleep(5)
                continue

            await ws.send(json.dumps({
                "type": "heartbeat",
                "payload": {"id": device_info["id"]}
            }))
            await asyncio.sleep(10) #TODO Consider extending the timer

        except Exception as e:
            print("Heartbeat error:", e)
            break


async def main_websocket():
    while True:  # auto-reconnect loop
        try:
            async with websockets.connect(URI) as ws:
                device_info = load_device_info()

                # Register device
                await ws.send(json.dumps({
                    "type": "register",
                    "payload": device_info
                }))
                response = json.loads(await ws.recv())
                if response.get("type") == "register_ack":
                    device_info["id"] = response["payload"]["id"]
                    save_device_info(device_info)
                    print("Registered with ID:", device_info["id"])

                    from device_controller import DeviceController

                    controller = DeviceController(ws, device_info)
                    await controller.initialize()
                else:
                    print("Registration failed:", response)
                    return

                # Run both loops concurrently
                await asyncio.gather(
                    receiver(ws, device_info, controller),
                    heartbeat(ws, device_info)
                )

        except Exception as e:
            print("Connection lost, retrying...", e)
            await asyncio.sleep(5) #TODO this timer needs to change later


# if __name__ == "__main__":
#     asyncio.run(main_websocket())
