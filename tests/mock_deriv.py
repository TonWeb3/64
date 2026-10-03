"""Tiny mock of Deriv's public WebSocket for local testing.

Run:  python tests/mock_deriv.py   (listens on ws://127.0.0.1:9999)
Then: DERIV_WS_URL=ws://127.0.0.1:9999 uvicorn main:app
"""
import asyncio
import json
import random
import time

import websockets

PORT = 9999
BASE = {"R_100": 1000.0, "1HZ100V": 1000.0, "R_75": 500.0, "1HZ75V": 500.0, "R_50": 250.0, "1HZ50V": 250.0}


async def handler(ws):
    sub = None
    price = None

    async def stream(symbol, req_id):
        nonlocal price
        price = BASE.get(symbol, 100.0)
        while True:
            await asyncio.sleep(0.2)  # fast for testing
            price = round(price + random.uniform(-0.5, 0.5), 2)
            await ws.send(json.dumps({
                "msg_type": "tick",
                "echo_req": {"ticks": symbol},
                "req_id": req_id,
                "tick": {"ask": price + 0.01, "bid": price - 0.01, "epoch": int(time.time()),
                         "id": "mock-" + str(random.randint(1000, 9999)), "pip_size": 2,
                         "quote": price, "symbol": symbol},
                "subscription": {"id": "mock-sub"},
            }))

    try:
        async for raw in ws:
            msg = json.loads(raw)
            if "ticks" in msg:
                if msg["ticks"] not in BASE:
                    await ws.send(json.dumps({"msg_type": "tick", "error": {"code": "InvalidSymbol", "message": "Symbol invalid"}, "echo_req": msg}))
                    continue
                sub = asyncio.create_task(stream(msg["ticks"], msg.get("req_id")))
            elif "ping" in msg:
                await ws.send(json.dumps({"msg_type": "ping", "ping": "pong"}))
    finally:
        if sub:
            sub.cancel()


async def main():
    async with websockets.serve(handler, "127.0.0.1", PORT):
        print(f"mock deriv on ws://127.0.0.1:{PORT}")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
