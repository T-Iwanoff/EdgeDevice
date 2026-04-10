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

async def test():
    uri = "ws://192.168.1.2:8000/ws"

    async with websockets.connect(uri) as ws:
        config = load_config()
        await ws.send(json.dumps(config))
        response = await ws.recv()
        data = json.loads(response)
        config["id"] = data.get("id")
        save_config(config)
        print("Server replied:", response)


asyncio.run(test())