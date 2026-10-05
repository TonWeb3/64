"""Deriv 10-Minute Window Auto-Trader

FastAPI app that connects to Deriv's public WebSocket, streams ticks for
synthetic indices, executes automated 10-minute window strategy learning and
trading, and records session data into CSV. Dashboard served at /.
"""
from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager
import csv
import json
import os
import re
import secrets
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import websockets
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import httpx

from bot import (
    Hub,
    REST_URL,
    SessionConfig,
    Settings,
    SettingsError,
    TradeError,
    TradingSession,
    utcnow_iso,
)

# --------------------------------------------------------------------------- config

BASE_DIR = Path(__file__).resolve().parent
DERIV_WS_URL = os.getenv("DERIV_WS_URL", "wss://api.derivws.com/trading/v1/options/ws/public")
DATA_DIR = Path(os.getenv("DATA_DIR", "./data")).resolve()
SETTINGS_FILE = Path(os.getenv("SETTINGS_FILE", BASE_DIR / "settings.json")).resolve()
DASHBOARD_USER = os.getenv("DASHBOARD_USER", "admin")
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "")  # empty = no auth
PING_INTERVAL = int(os.getenv("PING_INTERVAL", "30"))
MAX_BACKOFF = int(os.getenv("MAX_BACKOFF", "30"))
STALE_AFTER = int(os.getenv("STALE_AFTER", "20"))

# Live trading (demo / real). Paper trading needs none of this.
DERIV_TOKEN = os.getenv("DERIV_TOKEN", "")            # Personal Access Token with the `trade` scope
DERIV_APP_ID = os.getenv("DERIV_APP_ID", "")          # required for PAT auth
DERIV_ACCOUNT_ID = os.getenv("DERIV_ACCOUNT_ID", "")  # optional: pin a specific account
ALLOW_REAL_TRADING = os.getenv("ALLOW_REAL_TRADING", "") == "1"
HARD_MAX_STAKE = float(os.getenv("MAX_STAKE", "0") or 0)  # optional server-side ceiling on top of the settings page
MIN_STAKE = 0.35
MIN_WINDOW_SECONDS = float(os.getenv("MIN_WINDOW_SECONDS", "60"))

SYMBOLS: dict = {
    "R_100": "Volatility 100 Index",
    "1HZ100V": "Volatility 100 (1s) Index",
    "R_75": "Volatility 75 Index",
    "1HZ75V": "Volatility 75 (1s) Index",
    "R_50": "Volatility 50 Index",
    "1HZ50V": "Volatility 50 (1s) Index",
}

CSV_COLUMNS = [
    "received_at_utc", "tick_time_utc", "epoch", "symbol", "quote", "bid", "ask",
    "pip_size", "change", "tick_id", "cycle", "phase",
]

FILENAME_RE = re.compile(r"^(ticks|trades|cycles)_[A-Za-z0-9_]+_\d{8}_\d{6}\.csv$")

DATA_DIR.mkdir(parents=True, exist_ok=True)

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
        self.session: Optional[TradingSession] = None
        self._last_quote: Optional[float] = None
        self._fh = None
        self._writer = None

    def status(self) -> dict:
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
            "mode": self.session.cfg.mode if self.session else "demo",
            "session": self.session.status() if self.session else None,
        }

    def push_status(self) -> None:
        hub.publish("status", self.status())

    async def start(self, symbol: str, session: Optional[TradingSession], stamp: str) -> None:
        if self.running:
            raise RuntimeError("Already running")
        self.file = DATA_DIR / f"ticks_{symbol}_{stamp}.csv"
        self._fh = open(self.file, "w", newline="", encoding="utf-8")
        self._writer = csv.writer(self._fh)
        self._writer.writerow(CSV_COLUMNS)
        self._fh.flush()

        self.session = session
        self.symbol = symbol
        self.tick_count = 0
        self.reconnects = 0
        self.last_tick = None
        self._last_quote = None
        self.started_at = time.time()
        self.running = True
        hub.ticks.clear()
        hub.publish("reset", {"symbol": symbol})
        mode = session.cfg.mode if session else "demo"
        hub.log(f"Started {mode} auto-trader on {symbol} ({SYMBOLS[symbol]}) -> {self.file.name}")
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
        if self.session:
            await self.session.stop()
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

    async def _run(self, symbol: str) -> None:
        backoff = 1
        while self.running:
            try:
                hub.log(f"Connecting to {DERIV_WS_URL}")
                async with websockets.connect(
                    DERIV_WS_URL, open_timeout=15, ping_interval=20, ping_timeout=20, max_size=2**22
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
                        await asyncio.sleep(5)
                    return
                mt = msg.get("msg_type")
                if mt == "tick":
                    self._handle_tick(msg["tick"])
                elif mt != "ping":
                    hub.log(f"msg_type={mt}", "debug")
        finally:
            ping_task.cancel()

    async def _pinger(self, ws) -> None:
        try:
            while True:
                await asyncio.sleep(PING_INTERVAL)
                await ws.send(json.dumps({"ping": 1}))
        except Exception:
            pass

    def _handle_tick(self, t: dict) -> None:
        quote, epoch = t.get("quote"), t.get("epoch")
        if quote is None or epoch is None:
            return
        change = None if self._last_quote is None else round(quote - self._last_quote, 6)
        self._last_quote = quote

        cycle, phase = (1, "learn")
        if self.session:
            try:
                cycle, phase = self.session.on_tick(quote, epoch)
            except Exception as e:  # noqa: BLE001
                hub.log(f"Session error: {e!r}", "error")

        row = [
            utcnow_iso(),
            datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            epoch, t.get("symbol", self.symbol), quote, t.get("bid", ""), t.get("ask", ""),
            t.get("pip_size", ""), "" if change is None else change, t.get("id", ""), cycle, phase,
        ]
        if self._writer:
            self._writer.writerow(row)
            self._fh.flush()
        self.tick_count += 1
        point = {"epoch": epoch, "quote": quote, "change": change, "symbol": row[3],
                 "count": self.tick_count, "cycle": cycle, "phase": phase}
        self.last_tick = point
        hub.ticks.append(point)
        hub.publish("tick", point)
        if self.tick_count % 25 == 0:
            self.push_status()
        if self.tick_count == 1 or self.tick_count % 100 == 0:
            hub.log(f"{self.tick_count} ticks received (last {quote})")


collector = Collector()
settings = Settings(SETTINGS_FILE)


def eff_token() -> str:
    return settings.token or DERIV_TOKEN


def eff_app_id() -> str:
    return settings.data["app_id"] or DERIV_APP_ID


def eff_account_id() -> str:
    return settings.data["account_id"] or DERIV_ACCOUNT_ID


def real_allowed() -> bool:
    return ALLOW_REAL_TRADING or bool(settings.data["allow_real"])


def max_stake() -> float:
    m = settings.data["max_stake"]
    return min(m, HARD_MAX_STAKE) if HARD_MAX_STAKE > 0 else m

# --------------------------------------------------------------------------- app


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await collector.stop()


app = FastAPI(title="Deriv Tick Collector", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")


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
            return Response("Authentication required", status_code=401,
                            headers={"WWW-Authenticate": 'Basic realm="Deriv Tick Collector"'})
    return await call_next(request)


class StartBody(BaseModel):
    symbol: str
    mode: str = "auto"
    confirm_real: bool = False


class SettingsBody(BaseModel):
    token: Optional[str] = None
    clear_token: bool = False
    values: dict = {}


@app.get("/health")
async def health():
    return {"ok": True}


@app.get("/")
async def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/settings")
async def settings_page():
    return FileResponse(Path(__file__).parent / "static" / "settings.html")


def public_settings() -> dict:
    tok = eff_token()
    return {
        "values": settings.data,
        "token_set": bool(tok),
        "token_hint": ("\u2022\u2022\u2022\u2022" + tok[-4:]) if len(tok) >= 8 else "",
        "token_source": "settings" if settings.token else ("environment" if DERIV_TOKEN else ""),
        "app_id_source": "settings" if settings.data["app_id"] else ("environment" if DERIV_APP_ID else ""),
        "real_env": ALLOW_REAL_TRADING,
        "real_allowed": real_allowed(),
        "max_stake_cap": HARD_MAX_STAKE or None,
        "auth_enabled": bool(DASHBOARD_PASSWORD),
        "min_window_seconds": MIN_WINDOW_SECONDS,
        "running": collector.running,
    }


@app.get("/api/settings")
async def get_settings():
    return public_settings()


@app.put("/api/settings")
async def put_settings(body: SettingsBody):
    if collector.running:
        raise HTTPException(409, "Stop the session before changing settings.")
    v = dict(body.values)
    if "window_minutes" in v:
        try:
            if float(v["window_minutes"]) * 60 < MIN_WINDOW_SECONDS:
                raise HTTPException(400, f"window_minutes must be at least {MIN_WINDOW_SECONDS / 60:g} minutes")
        except (TypeError, ValueError):
            raise HTTPException(400, "window_minutes must be a number")
    if v.get("account_mode") == "real" and not (real_allowed() or v.get("allow_real") is True):
        raise HTTPException(400, "Turn on 'Allow live trading' before selecting the live account.")
    if v.get("allow_real") is False and v.get("account_mode", settings.data["account_mode"]) == "real":
        v["account_mode"] = "demo"
    if v.get("default_symbol") is not None and v["default_symbol"] not in SYMBOLS:
        raise HTTPException(400, "Unsupported symbol")
    try:
        settings.update({"token": body.token, **v}, clear_token=body.clear_token)
    except SettingsError as e:
        raise HTTPException(400, str(e))
    hub.log("Settings saved" + (" (token updated)" if body.token else ""), "info")
    return public_settings()


@app.post("/api/settings/test")
async def test_connection():
    """List the accounts the saved token can see (no trading, nothing bought)."""
    tok = eff_token()
    if not tok:
        raise HTTPException(400, "Save a token first.")
    headers = {"Authorization": f"Bearer {tok}"}
    if eff_app_id():
        headers["Deriv-App-ID"] = eff_app_id()
    try:
        async with httpx.AsyncClient(base_url=REST_URL.rstrip("/"), timeout=15) as c:
            r = await c.get("/trading/v1/options/accounts", headers=headers)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"Could not reach Deriv: {type(e).__name__}")
    if r.status_code != 200:
        raise HTTPException(400, f"Deriv rejected the token (HTTP {r.status_code}). Check the token scopes and the App ID.")
    body = r.json()
    data = body.get("data", body) if isinstance(body, dict) else body
    accts = data if isinstance(data, list) else data.get("accounts", [])
    return {"accounts": [
        {"id": a.get("account_id"), "type": a.get("account_type"), "currency": a.get("currency"),
         "balance": a.get("balance"), "status": a.get("status")} for a in accts]}


@app.get("/api/config")
async def config():
    has = bool(eff_token())
    d = settings.data
    return {
        "symbols": [{"id": k, "name": v} for k, v in SYMBOLS.items()],
        "modes": {"demo": has, "real": has and real_allowed()},
        "has_token": has,
        "real_allowed": real_allowed(),
        "account_mode": d["account_mode"],
        "default_symbol": d["default_symbol"],
        "summary": {
            "account_mode": d["account_mode"],
            "risk": (f"{d['risk_percent']:g}% of balance" if d["risk_mode"] == "percent" else f"{d['stake']:g} per trade"),
            "strictness": d["strictness"], "max_session_loss": d["max_session_loss"],
            "window_minutes": d.get("window_minutes", 10.0),
            "archive_interval_minutes": d.get("archive_interval_minutes", 60.0),
        },
    }


@app.get("/api/status")
async def status():
    return collector.status()


@app.post("/api/start")
async def start(body: StartBody):
    if body.symbol not in SYMBOLS:
        raise HTTPException(400, f"Unsupported symbol: {body.symbol}")
    if collector.running:
        raise HTTPException(409, "Already running")

    session: Optional[TradingSession] = None
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    d = settings.data
    mode = d["account_mode"]
    if not eff_token():
        raise HTTPException(400, "Add your Deriv token on the Settings page to trade.")
    if mode == "real":
        if not real_allowed():
            raise HTTPException(400, "Live trading is switched off. Turn on 'Allow live trading' in Settings.")
        if not body.confirm_real:
            raise HTTPException(400, "Confirm that you understand real money is at risk.")
    stake_cap = max_stake()
    if d["risk_mode"] == "fixed" and not (MIN_STAKE <= d["stake"] <= stake_cap):
        raise HTTPException(400, f"Stake must be between {MIN_STAKE} and {stake_cap:g}. Change it in Settings.")

    period = float(d.get("window_minutes", 10.0))
    if period * 60 < MIN_WINDOW_SECONDS:
        raise HTTPException(400, f"The window must be at least {MIN_WINDOW_SECONDS:g} seconds.")
    archive_secs = float(d.get("archive_interval_minutes", 60.0)) * 60

    session = TradingSession(
        SessionConfig(
            mode=mode, symbol=body.symbol, stake=d["stake"], window_seconds=period * 60,
            archive_seconds=archive_secs,
            strictness=d["strictness"], payout_ratio=d["payout_ratio"], min_trades=d["min_trades"],
            edge_margin=d["edge_margin"] / 100, max_trades_per_window=d["max_trades_per_window"],
            max_session_loss=max(MIN_STAKE, d["max_session_loss"]),
            risk_mode=d["risk_mode"], risk_percent=d["risk_percent"],
            min_stake=MIN_STAKE, max_stake=stake_cap, profit_target=d["profit_target"],
            max_consecutive_losses=d["max_consecutive_losses"],
        ),
        hub, DATA_DIR, stamp,
        deriv_env={"token": eff_token(), "app_id": eff_app_id(), "account_id": eff_account_id(), "ws_url": DERIV_WS_URL},
    )
    try:
        await session.start()
    except TradeError as e:
        hub.log(f"Could not start {mode} trading: {e}", "error")
        raise HTTPException(400, str(e))

    try:
        await collector.start(body.symbol, session, stamp)
    except RuntimeError as e:
        raise HTTPException(409, str(e))
    return collector.status()


@app.post("/api/stop")
async def stop():
    await collector.stop()
    return collector.status()


def _kind(p: Path) -> str:
    return p.name.split("_", 1)[0]


def _file_info(p: Path) -> dict:
    st = p.stat()
    active = False
    if collector.running:
        names = {collector.file.name if collector.file else None}
        if collector.session:
            names |= {collector.session.trades_path.name, collector.session.cycles_path.name}
        active = p.name in names
    return {
        "name": p.name,
        "kind": _kind(p),
        "size": st.st_size,
        "modified": datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat(),
        "active": active,
    }


def _all_files() -> list:
    items = [p for p in DATA_DIR.glob("*.csv") if FILENAME_RE.match(p.name)]
    items.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return items


@app.get("/api/files")
async def files():
    return [_file_info(p) for p in _all_files()]


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
async def download_current(kind: str = "ticks"):
    if kind not in ("ticks", "trades", "cycles"):
        raise HTTPException(400, "kind must be ticks, trades or cycles")
    p: Optional[Path] = None
    if kind == "ticks" and collector.file:
        p = collector.file
    elif collector.session:
        p = {"trades": collector.session.trades_path, "cycles": collector.session.cycles_path}.get(kind)
    if p is None or not p.exists():
        candidates = [x for x in _all_files() if _kind(x) == kind]
        if not candidates:
            raise HTTPException(404, f"No {kind} data collected yet")
        p = candidates[0]
    return FileResponse(p, media_type="text/csv", filename=p.name)


@app.delete("/api/files/{name}")
async def delete_file(name: str):
    p = _safe_path(name)
    if _file_info(p)["active"]:
        raise HTTPException(409, "Stop the active session before deleting this file")
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
            yield sse("snapshot", {
                "logs": list(hub.logs),
                "ticks": list(hub.ticks),
                "session": collector.session.snapshot() if collector.session else None,
            })
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
        stream(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port)

