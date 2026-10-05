"""Trade execution: paper (simulated from live ticks) and Deriv (demo / real).

Both expose the same small interface used by the session:

    await trader.start()
    await trader.open(trade)        # returns quickly; settles later via on_settle(trade)
    trader.on_tick(quote, epoch)    # paper settles here; no-op for Deriv
    await trader.finish(timeout)    # at stop: wait for / void open trades
    await trader.stop()
"""
from __future__ import annotations

import asyncio
import itertools
import json
import os
from dataclasses import dataclass, field, asdict
from typing import Callable, Optional

import httpx
import websockets

from bot.hub import utcnow_iso

REST_URL = os.getenv("DERIV_REST_URL", "https://api.derivws.com")


class TradeError(Exception):
    pass


@dataclass
class Trade:
    id: int
    cycle: int
    mode: str
    symbol: str
    side: str                  # CALL (rise) / PUT (fall)
    expiry: int                # ticks
    stake: float
    rule: str
    reason: str
    signal_quote: float
    opened_at: str = field(default_factory=utcnow_iso)
    signal_epoch: Optional[int] = None   # tick the signal fired on (used to place the chart marker)
    entry_spot: Optional[float] = None
    exit_spot: Optional[float] = None
    result: str = "open"       # open | won | lost | void | unknown
    profit: float = 0.0
    contract_id: Optional[int] = None
    settled_at: Optional[str] = None
    note: str = ""
    _done: bool = field(default=False, repr=False)

    def to_dict(self) -> dict:
        d = asdict(self)
        d.pop("_done", None)
        return d


class PaperTrader:
    """Simulated trades settled from the same live ticks that are being recorded.

    Settlement matches engine.settle: entry is the first tick after the signal,
    exit is `expiry` ticks after entry, a tie is a loss.
    """

    mode = "paper"

    def __init__(self, payout_ratio: float, on_settle: Callable[[Trade], None]):
        self.payout_ratio = payout_ratio
        self.on_settle = on_settle
        self._pending: list = []

    async def start(self) -> None:
        pass

    async def open(self, trade: Trade) -> None:
        self._pending.append({"trade": trade, "seen": -1})

    def on_tick(self, quote: float, epoch: int) -> None:
        for p in list(self._pending):
            t: Trade = p["trade"]
            if t.entry_spot is None:
                t.entry_spot = quote
                p["seen"] = 0
                continue
            p["seen"] += 1
            if p["seen"] >= t.expiry:
                t.exit_spot = quote
                won = (quote > t.entry_spot) if t.side == "CALL" else (quote < t.entry_spot)
                t.result = "won" if won else "lost"
                t.profit = round(t.stake * self.payout_ratio, 2) if won else -t.stake
                t.settled_at = utcnow_iso()
                self._pending.remove(p)
                self.on_settle(t)

    async def finish(self, timeout: float = 0) -> None:
        for p in list(self._pending):
            t: Trade = p["trade"]
            t.result, t.note, t.settled_at = "void", "session stopped before expiry", utcnow_iso()
            self._pending.remove(p)
            self.on_settle(t)

    async def stop(self) -> None:
        pass


class DerivTrader:
    """Rise/Fall tick contracts on a Deriv demo or real account.

    Flow: REST accounts list (checks the account type matches the requested
    mode) -> REST OTP -> authenticated WebSocket -> proposal -> buy ->
    proposal_open_contract until sold.
    """

    def __init__(self, mode: str, log: Callable[[str, str], None], on_settle: Callable[[Trade], None],
                 token: str, app_id: str = "", account_id: str = "", rest_url: str = REST_URL):
        assert mode in ("demo", "real")
        self.mode = mode
        self.log = log
        self.on_settle = on_settle
        self.token = token
        self.app_id = app_id
        self.account_id = account_id
        self.rest_url = rest_url.rstrip("/")
        self.currency = "USD"
        self.payout_ratio: Optional[float] = None  # learned from real proposals
        self.balance: Optional[float] = None
        self._client: Optional[httpx.AsyncClient] = None
        self._ws = None
        self._reader: Optional[asyncio.Task] = None
        self._pinger: Optional[asyncio.Task] = None
        self._ids = itertools.count(1)
        self._handlers: dict = {}
        self._open: dict = {}      # trade id -> Trade
        self._lock = asyncio.Lock()

    # ---- setup
    def _headers(self) -> dict:
        h = {"Authorization": f"Bearer {self.token}"}
        if self.app_id:
            h["Deriv-App-ID"] = self.app_id
        return h

    async def start(self) -> None:
        self._client = httpx.AsyncClient(base_url=self.rest_url, timeout=15)
        await self._resolve_account()
        await self._connect()

    async def _resolve_account(self) -> None:
        r = await self._client.get("/trading/v1/options/accounts", headers=self._headers())
        if r.status_code != 200:
            raise TradeError(f"Could not list Deriv accounts (HTTP {r.status_code}): {r.text[:200]}")
        body = r.json()
        data = body.get("data", body) if isinstance(body, dict) else body
        accounts = data if isinstance(data, list) else data.get("accounts", [])
        pool = [a for a in accounts if a.get("account_type") == self.mode and a.get("status", "active") == "active"]
        if self.account_id:
            match = [a for a in accounts if a.get("account_id") == self.account_id]
            if not match:
                raise TradeError(f"Account {self.account_id} not found for this token.")
            if match[0].get("account_type") != self.mode:
                raise TradeError(
                    f"Account {self.account_id} is a {match[0].get('account_type')} account but mode is '{self.mode}'. Refusing to trade."
                )
            pool = match
        if not pool:
            raise TradeError(f"No active {self.mode} account found for this token.")
        acct = pool[0]
        self.account_id = acct["account_id"]
        self.currency = acct.get("currency", "USD")
        self.balance = acct.get("balance")
        self.log(f"Using {self.mode} account {self.account_id} ({self.currency}, balance {self.balance})", "info")

    async def _connect(self) -> None:
        r = await self._client.post(f"/trading/v1/options/accounts/{self.account_id}/otp", headers=self._headers())
        if r.status_code != 200:
            raise TradeError(f"OTP request failed (HTTP {r.status_code}): {r.text[:200]}")
        url = r.json()["data"]["url"]
        self._ws = await websockets.connect(url, open_timeout=15, ping_interval=20, ping_timeout=20)
        self._reader = asyncio.create_task(self._read_loop(self._ws))
        self._pinger = asyncio.create_task(self._ping_loop(self._ws))
        self.log("Trading WebSocket connected", "info")

    async def _ensure(self) -> None:
        async with self._lock:
            if self._ws is None:
                self.log("Trading WebSocket was down, reconnecting with a new OTP", "warn")
                await self._connect()

    async def _read_loop(self, ws) -> None:
        try:
            async for raw in ws:
                msg = json.loads(raw)
                h = self._handlers.get(msg.get("req_id"))
                if h:
                    h(msg)
        except Exception as e:  # noqa: BLE001
            self.log(f"Trading WebSocket closed: {e!r}", "warn")
        finally:
            if self._ws is ws:
                self._ws = None
            for h in list(self._handlers.values()):
                h({"error": {"code": "Disconnected", "message": "trading socket closed"}})

    async def _ping_loop(self, ws) -> None:
        try:
            while True:
                await asyncio.sleep(30)
                await ws.send(json.dumps({"ping": 1}))
        except Exception:  # noqa: BLE001
            pass

    async def _call(self, payload: dict, timeout: float = 10) -> dict:
        await self._ensure()
        rid = next(self._ids)
        payload = {**payload, "req_id": rid}
        fut = asyncio.get_running_loop().create_future()

        def handler(msg):
            if not fut.done():
                fut.set_result(msg)

        self._handlers[rid] = handler
        try:
            await self._ws.send(json.dumps(payload))
            msg = await asyncio.wait_for(fut, timeout)
        finally:
            self._handlers.pop(rid, None)
        if msg.get("error"):
            e = msg["error"]
            raise TradeError(f"{e.get('code')}: {e.get('message')}")
        return msg

    async def probe_payout(self, symbol: str, stake: float, expiry: int = 5) -> Optional[float]:
        """Ask for a quote (nothing is bought) to learn the real payout ratio."""
        try:
            prop = await self._call({
                "proposal": 1, "amount": stake, "basis": "stake", "contract_type": "CALL",
                "currency": self.currency, "duration": expiry, "duration_unit": "t",
                "underlying_symbol": symbol,
            })
            p = prop["proposal"]
            ask, payout = float(p["ask_price"]), float(p["payout"])
            if ask > 0:
                self.payout_ratio = round(payout / ask - 1, 4)
            return self.payout_ratio
        except Exception as e:  # noqa: BLE001
            self.log(f"Could not read the live payout ({e}); using the assumed payout until the first trade", "warn")
            return None

    def account_info(self) -> dict:
        return {"id": self.account_id, "type": self.mode, "currency": self.currency, "balance": self.balance}

    # ---- trading
    async def open(self, trade: Trade) -> None:
        self._open[trade.id] = trade
        try:
            buy = None
            try:
                # 1-step direct buy: eliminates 1 entire network round-trip (~300-600ms faster execution)
                buy = await self._call({
                    "buy": 1, "price": trade.stake,
                    "parameters": {
                        "amount": trade.stake, "basis": "stake", "contract_type": trade.side,
                        "currency": self.currency, "duration": trade.expiry, "duration_unit": "t",
                        "symbol": trade.symbol,
                    }
                })
            except Exception:
                # Fallback to two-step proposal -> buy if broker requires proposal ID
                prop = await self._call({
                    "proposal": 1, "amount": trade.stake, "basis": "stake", "contract_type": trade.side,
                    "currency": self.currency, "duration": trade.expiry, "duration_unit": "t",
                    "underlying_symbol": trade.symbol,
                })
                p = prop["proposal"]
                ask, payout = float(p["ask_price"]), float(p["payout"])
                if ask > 0:
                    self.payout_ratio = round(payout / ask - 1, 4)
                buy = await self._call({"buy": p["id"], "price": ask})

            trade.contract_id = int(buy["buy"]["contract_id"])
            if buy["buy"].get("balance_after") is not None:
                self.balance = float(buy["buy"]["balance_after"])
            payout_txt = f", payout x{self.payout_ratio}" if self.payout_ratio else ""
            self.log(f"Bought {trade.side} {trade.expiry}t on {trade.symbol} (contract {trade.contract_id}{payout_txt})", "info")
            self._watch(trade)
            asyncio.create_task(self._watchdog(trade))
        except Exception as e:  # noqa: BLE001
            trade.result, trade.note = "void", f"order failed: {e}"
            self._finish(trade)

    def _watch(self, trade: Trade) -> None:
        rid = next(self._ids)

        def handler(msg):
            if msg.get("error"):
                if msg["error"].get("code") == "Disconnected":
                    return  # the watchdog reports unknown outcomes
                self.log(f"Contract {trade.contract_id} stream error: {msg['error']}", "warn")
                return
            poc = msg.get("proposal_open_contract")
            if not poc:
                return
            if poc.get("entry_spot") not in (None, "") and trade.entry_spot is None:
                trade.entry_spot = float(poc["entry_spot"])
            if poc.get("is_sold") or poc.get("status") in ("won", "lost"):
                if poc.get("exit_spot") not in (None, ""):
                    trade.exit_spot = float(poc["exit_spot"])
                trade.profit = float(poc.get("profit", 0) or 0)
                trade.result = "won" if (poc.get("status") == "won" or trade.profit > 0) else "lost"
                if trade.result == "won" and self.balance is not None:
                    self.balance = round(self.balance + trade.stake + trade.profit, 2)
                sub = (msg.get("subscription") or {}).get("id")
                self._handlers.pop(rid, None)
                if sub and self._ws is not None:
                    asyncio.create_task(self._forget(sub))
                self._finish(trade)

        self._handlers[rid] = handler
        asyncio.create_task(self._send_raw({"proposal_open_contract": 1, "contract_id": trade.contract_id,
                                            "subscribe": 1, "req_id": rid}))

    async def _send_raw(self, payload: dict) -> None:
        try:
            await self._ws.send(json.dumps(payload))
        except Exception as e:  # noqa: BLE001
            self.log(f"send failed: {e!r}", "warn")

    async def _forget(self, sub_id: str) -> None:
        await self._send_raw({"forget": sub_id})

    async def _watchdog(self, trade: Trade) -> None:
        await asyncio.sleep((trade.expiry + 5) * 3 + 20)
        if not trade._done:
            trade.result = "unknown"
            trade.note = "no settlement received, check the contract in your Deriv statement"
            self._finish(trade)

    def _finish(self, trade: Trade) -> None:
        if trade._done:
            return
        trade.settled_at = utcnow_iso()
        self._open.pop(trade.id, None)
        self.on_settle(trade)

    def on_tick(self, quote: float, epoch: int) -> None:
        pass

    async def finish(self, timeout: float = 20) -> None:
        end = asyncio.get_running_loop().time() + timeout
        while self._open and asyncio.get_running_loop().time() < end:
            await asyncio.sleep(0.5)
        for t in list(self._open.values()):
            t.result, t.note = "unknown", "session stopped while contract was open, check your Deriv statement"
            self._finish(t)

    async def stop(self) -> None:
        for task in (self._reader, self._pinger):
            if task:
                task.cancel()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:  # noqa: BLE001
                pass
            self._ws = None
        if self._client:
            await self._client.aclose()
