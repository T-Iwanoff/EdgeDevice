import asyncio
from src.EdgeDevice import dashboard_connection

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    while True:
        asyncio.run(dashboard_connection.main_websocket())



