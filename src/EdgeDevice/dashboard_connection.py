import asyncio
import websockets
import json

async def test():
    uri = "ws://192.168.1.2:8000/ws"

    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({
            "name": "Device X",
            "id": None,
            "status": "idle"
        }))

        response = await ws.recv()
        print("Server replied:", response)

asyncio.run(test())