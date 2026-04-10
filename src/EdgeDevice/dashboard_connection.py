import asyncio
import websockets
import json
from pathlib import Path

CONFIG_PATH = Path("device.json")

def load_config():
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {"name": "Device X", "id": None, "status": "idle"}

def save_config(config):
    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)

# Handles ALL incoming messages from server
async def receiver(ws, config):
    while True:
        try:
            raw = await ws.recv()
            msg = json.loads(raw)
            print("Received:", msg)
            msg_type = msg.get("type")

            if msg_type == "set_status":
                new_status = msg["payload"]["status"]
                config["status"] = new_status
                save_config(config)
                print(f"Status updated to {new_status}")

                # Notify server about change
                await ws.send(json.dumps({
                    "type": "update_status",
                    "payload": config
                }))

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
            await asyncio.sleep(10) #TODO timer needs to be extended

        except Exception as e:
            print("Heartbeat error:", e)
            break


async def main_websocket():
    uri = "ws://192.168.1.2:8000/ws"

    while True:  # auto-reconnect loop
        try:
            async with websockets.connect(uri) as ws:
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
                else:
                    print("Registration failed:", response)
                    return

                # Run both loops concurrently
                await asyncio.gather(
                    receiver(ws, config),
                    heartbeat(ws, config)
                )

        except Exception as e:
            print("Connection lost, retrying...", e)
            await asyncio.sleep(5) #TODO this timer needs to change later


if __name__ == "__main__":
    asyncio.run(main_websocket())
