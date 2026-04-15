import asyncio
import websockets
import json
import aiohttp
from pathlib import Path


URI = "ws://192.168.1.2:8000/ws" #TODO edit during setup

CONFIG_PATH = Path("device.json")
DATA_DIR = Path(__file__).resolve().parents[2] / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def load_config():
    if CONFIG_PATH.exists():
        return json.load(open(CONFIG_PATH))
    return {"name": "Device X", "id": None, "status": "idle"}

def save_config(config):
    CONFIG_PATH.write_text(json.dumps(config, indent=2))

async def download_file(url, file_id, bucket, ws, config):
    path = DATA_DIR / bucket
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
        # notify server
        await ws.send(json.dumps({
            "type": "download_complete",
            "payload": {
                "file_id": file_id,
                "bucket": bucket,
                "id": config["id"]
            }
        }))
    except Exception as e:
        print("Download failed:", e)


# Handles ALL incoming messages from server
async def receiver(ws, config, controller):
    while True:
        try:
            raw = await ws.recv()
            msg = json.loads(raw)
            print("Received:", msg)
            msg_type = msg.get("type")

            if msg_type == "get_status":
                await ws.send(json.dumps({
                    "type": "update_status",
                    "payload": config
                }))

            elif msg_type == "download_file":
                payload = msg.get("payload", {})
                url = payload.get("url")
                file_id = payload.get("file_id")
                bucket = payload.get("bucket", "training_data")
                if url and file_id:
                    asyncio.create_task(download_file(url, file_id, bucket, ws, config))
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
async def heartbeat(ws, config):
    while True:
        try:
            if not config.get("id"):
                print("No device ID, skipping heartbeat")
                await asyncio.sleep(5)
                continue

            await ws.send(json.dumps({
                "type": "heartbeat",
                "payload": {"id": config["id"]}
            }))
            await asyncio.sleep(10) #TODO Consider extending the timer

        except Exception as e:
            print("Heartbeat error:", e)
            break


async def main_websocket():
    while True:  # auto-reconnect loop
        try:
            async with websockets.connect(URI) as ws:
                config = load_config()

                # Register device
                await ws.send(json.dumps({
                    "type": "register",
                    "payload": config
                }))
                response = json.loads(await ws.recv())
                if response.get("type") == "register_ack":
                    config["id"] = response["payload"]["id"]
                    save_config(config)
                    print("Registered with ID:", config["id"])

                    from device_controller import DeviceController

                    controller = DeviceController(ws, config)
                    await controller.initialize()
                else:
                    print("Registration failed:", response)
                    return

                # Run both loops concurrently
                await asyncio.gather(
                    receiver(ws, config, controller),
                    heartbeat(ws, config)
                )

        except Exception as e:
            print("Connection lost, retrying...", e)
            await asyncio.sleep(5) #TODO this timer needs to change later


if __name__ == "__main__":
    asyncio.run(main_websocket())
