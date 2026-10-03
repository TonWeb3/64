"""Deriv Tick Collector

A small FastAPI app that connects to Deriv's public WebSocket, records tick data
for one synthetic index at a time into a CSV file, and serves a dashboard with
Start/Stop, symbol switching, live logs and CSV download.
"""
from __future__ import annotations

import asyncio
import base64
import csv
import json
import logging
import os
import re
import secrets
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import websockets
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from pydantic import BaseModel

# --------------------------------------------------------------------------- config

DERIV_WS_URL = os.getenv("DERIV_WS_URL", "wss://api.derivws.com/trading/v1/options/ws/public")
DATA_DIR = Path(os.getenv("DATA_DIR", "./data")).resolve()
DASHBOARD_USER = os.getenv("DASHBOARD_USER", "admin")
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "")  # empty = no auth
PING_INTERVAL = int(os.getenv("PING_INTERVAL", "30"))
MAX_BACKOFF = int(os.getenv("MAX_BACKOFF", "30"))
STALE_AFTER = int(os.getenv("STALE_AFTER", "20"))  # seconds without a tick -> reconnect

SYMBOLS: dict[str, str] = {
    "R_100": "Volatility 100 Index",
    "1HZ100V": "Volatility 100 (1s) Index",
    "R_75": "Volatility 75 Index",
    "1HZ75V": "Volatility 75 (1s) Index",
    "R_50": "Volatility 50 Index",
    "1HZ50V": "Volatility 50 (1s) Index",
}

CSV_COLUMNS = [
    "received_at_utc",
    "tick_time_utc",
    "epoch",
    "symbol",
    "quote",
    "bid",
    "ask",
    "pip_size",
    "change",
    "tick_id",
]

FILENAME_RE = re.compile(r"^ticks_[A-Za-z0-9_]+_\d{8}_\d{6}\.csv$")

DATA_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("collector")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# --------------------------------------------------------------------------- event hub


class Hub:
    """Fan-out of events (logs, ticks, status) to connected dashboards via SSE."""

    def __init__(self) -> None:
        self.clients: set[asyncio.Queue] = set()
        self.logs: deque[dict] = deque(maxlen=500)
        self.ticks: deque[dict] = deque(maxlen=300)

    def publish(self, event: str, data: dict) -> None:
        payload = {"event": event, "data": data}
        for q in list(self.clients):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                # Slow client: drop the oldest item to make room.
                try:
                    q.get_nowait()
                    q.put_nowait(payload)
                except Exception:
                    pass

    def log(self, message: str, level: str = "info") -> None:
        entry = {"ts": utcnow_iso(), "level": level, "message": message}
        self.logs.append(entry)
        getattr(logger, "warning" if level == "warn" else level if level in ("info", "error") else "info")(message)
        self.publish("log", entry)


hub = Hub()


# --------------------------------------------------------------------------- collector


class Collector:
    def __init__(self) -> None:
        self.task: Optional[asyncio.Task] = None
        self.running = False
        self.symbol: Optional[str] = None
        self.file: Optional[Path] = None
        self.tick_count = 0
        self.started_at: Optional[float] = None
        self.connected = False
        self.last_tick: Optional[dict] = None
        self.reconnects = 0
        self._last_quote: Optional[float] = None
        self._fh = None
        self._writer = None

    # ---- state exposed to the dashboard
    def status(self) -> dict[str, Any]:
        return {
            "running": self.running,
            "connected": self.connected,
            "symbol": self.symbol,
            "file": self.file.name if self.file else None,
            "tick_count": self.tick_count,
            "started_at": self.started_at,
            "reconnects": self.reconnects,
            "last_tick": self.last_tick,
            "ws_url": DERIV_WS_URL,
        }

    def push_status(self) -> None:
        hub.publish("status", self.status())

    # ---- lifecycle
    async def start(self, symbol: str) -> None:
        if self.running:
            raise RuntimeError("Already running")
        if symbol not in SYMBOLS:
            raise ValueError(f"Unsupported symbol: {symbol}")

        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.file = DATA_DIR / f"ticks_{symbol}_{stamp}.csv"
        self._fh = open(self.file, "w", newline="", encoding="utf-8")
        self._writer = csv.writer(self._fh)
        self._writer.writerow(CSV_COLUMNS)
        self._fh.flush()

        self.symbol = symbol
        self.tick_count = 0
        self.reconnects = 0
        self.last_tick = None
        self._last_quote = None
        self.started_at = time.time()
        self.running = True
        hub.ticks.clear()
        hub.publish("reset", {"symbol": symbol})
        hub.log(f"Started recording {symbol} ({SYMBOLS[symbol]}) -> {self.file.name}")
        self.push_status()
        self.task = asyncio.create_task(self._run(symbol), name="collector")

    async def stop(self) -> None:
        if not self.running:
            return
        hub.log("Stopping...")
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except (asyncio.CancelledError, Exception):
                pass
            self.task = None
        self._close_file()
        self.connected = False
        hub.log(f"Stopped. {self.tick_count} ticks saved to {self.file.name if self.file else '-'}")
        self.push_status()

    def _close_file(self) -> None:
        if self._fh:
            try:
                self._fh.flush()
                self._fh.close()
            except Exception:
                pass
        self._fh = None
        self._writer = None

    # ---- main loop with reconnect
    async def _run(self, symbol: str) -> None:
        backoff = 1
        while self.running:
            try:
                hub.log(f"Connecting to {DERIV_WS_URL}")
                async with websockets.connect(
                    DERIV_WS_URL,
                    open_timeout=15,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=2**22,
                ) as ws:
                    self.connected = True
                    backoff = 1
                    hub.log("WebSocket connected")
                    self.push_status()
                    await ws.send(json.dumps({"ticks": symbol, "subscribe": 1, "req_id": 1}))
                    hub.log(f"Subscribed to ticks for {symbol}")
                    await self._receive(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                hub.log(f"Connection problem: {exc!r}", "warn")
            finally:
                self.connected = False
                self.push_status()

            if not self.running:
                break
            self.reconnects += 1
            hub.log(f"Reconnecting in {backoff}s (attempt {self.reconnects})", "warn")
            self.push_status()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)

    async def _receive(self, ws) -> None:
        ping_task = asyncio.create_task(self._pinger(ws))
        try:
            while self.running:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=STALE_AFTER)
                except asyncio.TimeoutError:
                    hub.log(f"No data for {STALE_AFTER}s, reconnecting", "warn")
                    return
                msg = json.loads(raw)
                if msg.get("error"):
                    err = msg["error"]
                    hub.log(f"API error: {err.get('code')}: {err.get('message')}", "error")
                    if err.get("code") in ("InvalidSymbol", "MarketIsClosed"):
                        # Not recoverable by reconnecting immediately; keep trying slowly.
                        await asyncio.sleep(5)
                    return
                mt = msg.get("msg_type")
                if mt == "tick":
                    self._handle_tick(msg["tick"])
                elif mt == "ping":
                    pass
                else:
                    hub.log(f"msg_type={mt}", "debug")
        finally:
            ping_task.cancel()

    async def _pinger(self, ws) -> None:
        try:
            while True:
                await asyncio.sleep(PING_INTERVAL)
                await ws.send(json.dumps({"ping": 1}))
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    def _handle_tick(self, t: dict) -> None:
        quote = t.get("quote")
        epoch = t.get("epoch")
        if quote is None or epoch is None:
            return
        change = None if self._last_quote is None else round(quote - self._last_quote, 6)
        self._last_quote = quote
        row = [
            utcnow_iso(),
            datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            epoch,
            t.get("symbol", self.symbol),
            quote,
            t.get("bid", ""),
            t.get("ask", ""),
            t.get("pip_size", ""),
            "" if change is None else change,
            t.get("id", ""),
        ]
        if self._writer:
            self._writer.writerow(row)
            self._fh.flush()
        self.tick_count += 1
        point = {
            "epoch": epoch,
            "quote": quote,
            "change": change,
            "symbol": row[3],
            "count": self.tick_count,
        }
        self.last_tick = point
        hub.ticks.append(point)
        hub.publish("tick", point)
        if self.tick_count % 25 == 0:
            self.push_status()
        if self.tick_count == 1 or self.tick_count % 100 == 0:
            hub.log(f"{self.tick_count} ticks recorded (last {quote})")


collector = Collector()

# --------------------------------------------------------------------------- app

app = FastAPI(title="Deriv Tick Collector")


@app.middleware("http")
async def basic_auth(request: Request, call_next):
    if DASHBOARD_PASSWORD and request.url.path != "/health":
        header = request.headers.get("authorization", "")
        ok = False
        if header.lower().startswith("basic "):
            try:
                user, _, pw = base64.b64decode(header[6:]).decode().partition(":")
                ok = secrets.compare_digest(user, DASHBOARD_USER) and secrets.compare_digest(pw, DASHBOARD_PASSWORD)
            except Exception:
                ok = False
        if not ok:
            return Response(
                "Authentication required",
                status_code=401,
                headers={"WWW-Authenticate": 'Basic realm="Deriv Tick Collector"'},
            )
    return await call_next(request)


class StartBody(BaseModel):
    symbol: str


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/")
async def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/api/symbols")
async def symbols():
    return [{"id": k, "name": v} for k, v in SYMBOLS.items()]


@app.get("/api/status")
async def status():
    return collector.status()


@app.post("/api/start")
async def start(body: StartBody):
    try:
        await collector.start(body.symbol)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return collector.status()


@app.post("/api/stop")
async def stop():
    await collector.stop()
    return collector.status()


def _file_info(p: Path) -> dict:
    st = p.stat()
    return {
        "name": p.name,
        "size": st.st_size,
        "modified": datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat(),
        "active": collector.running and collector.file is not None and collector.file.name == p.name,
    }


@app.get("/api/files")
async def files():
    items = [p for p in DATA_DIR.glob("ticks_*.csv") if FILENAME_RE.match(p.name)]
    items.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return [_file_info(p) for p in items]


def _safe_path(name: str) -> Path:
    if not FILENAME_RE.match(name):
        raise HTTPException(400, "Invalid file name")
    p = (DATA_DIR / name).resolve()
    if p.parent != DATA_DIR or not p.exists():
        raise HTTPException(404, "File not found")
    return p


@app.get("/api/download/{name}")
async def download(name: str):
    p = _safe_path(name)
    return FileResponse(p, media_type="text/csv", filename=p.name)


@app.get("/api/download-current")
async def download_current():
    if collector.file and collector.file.exists():
        p = collector.file
    else:
        items = [x for x in DATA_DIR.glob("ticks_*.csv") if FILENAME_RE.match(x.name)]
        if not items:
            raise HTTPException(404, "No data recorded yet")
        p = max(items, key=lambda x: x.stat().st_mtime)
    return FileResponse(p, media_type="text/csv", filename=p.name)


@app.delete("/api/files/{name}")
async def delete_file(name: str):
    p = _safe_path(name)
    if collector.running and collector.file and collector.file.name == p.name:
        raise HTTPException(409, "Stop recording before deleting the active file")
    p.unlink()
    hub.log(f"Deleted {name}")
    return {"deleted": name}


@app.get("/events")
async def events(request: Request):
    queue: asyncio.Queue = asyncio.Queue(maxsize=2000)
    hub.clients.add(queue)

    def sse(event: str, data: Any) -> str:
        return f"event: {event}\ndata: {json.dumps(data)}\n\n"

    async def stream():
        try:
            yield "retry: 3000\n\n"
            yield sse("status", collector.status())
            yield sse("snapshot", {"logs": list(hub.logs), "ticks": list(hub.ticks)})
            while True:
                if await request.is_disconnected():
                    break
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=15)
                    yield sse(item["event"], item["data"])
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            hub.clients.discard(queue)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


@app.on_event("shutdown")
async def on_shutdown():
    await collector.stop()
