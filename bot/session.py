"""Continuous Adaptive 10-Minute Learning & Trading Session.

Architecture:
- Window 1 (first 10 mins): Baseline gathering only. Ticks are collected without opening trades.
- Windows 2+ (every 10 mins): The engine backtests 64 candidate rules, updates its persistent
  cumulative KnowledgeBase, and trades the best-performing rule while continuously gathering ticks.
- Every 1 hour: Archives data and purges raw in-memory tick queues to preserve server memory
  without forgetting learned model parameters and rule performance track records.
"""
from __future__ import annotations

import asyncio
import csv
import os
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from bot.engine import FeatureTracker, KnowledgeBase, Rule, select_strategy
from bot.hub import Hub, utcnow_iso
from bot.trader import DerivTrader, PaperTrader, Trade, TradeError

NULL_SHUFFLES = int(os.getenv("NULL_SHUFFLES", "30"))

TRADE_COLUMNS = [
    "trade_id", "cycle", "opened_at_utc", "settled_at_utc", "symbol", "mode", "side", "expiry_ticks",
    "stake", "rule", "reason", "signal_quote", "entry_spot", "exit_spot", "result", "profit",
    "contract_id", "note",
]
CYCLE_COLUMNS = [
    "cycle", "phase", "ticks", "rule_used", "trades", "wins", "losses", "pnl",
    "learned_rule", "learned_win_rate", "learned_n", "breakeven", "luck_p90", "decision",
]


@dataclass
class SessionConfig:
    mode: str                         # demo | real
    symbol: str
    stake: float = 1.0
    window_seconds: float = 600.0     # 10 minutes per learning/trading cycle
    archive_seconds: float = 3600.0   # 1 hour periodic archival & memory cleanup
    strictness: str = "strict"        # strict | normal | always
    payout_ratio: float = 0.95
    min_trades: int = 8
    edge_margin: float = 0.02
    max_trades_per_window: int = 20
    max_session_loss: float = 10.0
    risk_mode: str = "fixed"          # fixed | percent
    risk_percent: float = 1.0
    min_stake: float = 0.35
    max_stake: float = 10.0
    profit_target: float = 0.0        # 0 = off
    max_consecutive_losses: int = 0   # 0 = off


class TradingSession:
    def __init__(self, cfg: SessionConfig, hub: Hub, data_dir: Path, stamp: str, deriv_env: Optional[dict] = None):
        self.cfg = cfg
        self.hub = hub
        self.stamp = stamp
        self.deriv_env = deriv_env or {}
        self.trades_path = data_dir / f"trades_{cfg.symbol}_{stamp}.csv"
        self.cycles_path = data_dir / f"cycles_{cfg.symbol}_{stamp}.csv"

        self.active = False
        self.tracker = FeatureTracker()
        self.kb = KnowledgeBase()
        self.cycle = 1
        self.phase = "learning"
        self.window_end = 0.0
        self.next_archive = 0.0
        self.archive_count = 0
        self.window_quotes: list[float] = []
        self.hourly_buf: deque = deque(maxlen=4000)
        self.trades_in_window = 0
        self.rule: Optional[Rule] = None
        self.strategy: Optional[dict] = None
        self.busy = False
        self.halted = False
        self.halt_reason = ""
        self.consec_losses = 0

        self.trader = None
        self.pnl = 0.0
        self.n_trades = self.wins = self.losses = self.unknown = 0
        self.trades: list = []
        self.cycles: dict = {}
        self._learn_task: Optional[asyncio.Task] = None
        self._next_id = 1
        self._fh = None
        self._writer = None

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        cfg = self.cfg
        if cfg.mode == "paper":
            self.trader = PaperTrader(cfg.payout_ratio, self._settled)
        else:
            self.trader = DerivTrader(
                cfg.mode, self.hub.log, self._settled,
                token=self.deriv_env.get("token", ""), app_id=self.deriv_env.get("app_id", ""),
                account_id=self.deriv_env.get("account_id", ""),
            )
        try:
            await self.trader.start()
        except TradeError:
            await self.trader.stop()
            raise
        except Exception as e:  # noqa: BLE001
            await self.trader.stop()
            raise TradeError(str(e)) from e

        if isinstance(self.trader, DerivTrader):
            live = await self.trader.probe_payout(cfg.symbol, cfg.stake)
            if live:
                self.hub.log(f"Live payout for {cfg.symbol} rise/fall: x{live} (breakeven win rate {1 / (1 + live):.1%})")

        self._fh = open(self.trades_path, "w", newline="", encoding="utf-8")
        self._writer = csv.writer(self._fh)
        self._writer.writerow(TRADE_COLUMNS)
        self._fh.flush()

        now = time.time()
        self.cycles[1] = self._new_cycle(1, "learn")
        self.window_end = now + cfg.window_seconds
        self.next_archive = now + cfg.archive_seconds

        self.hub.log(
            f"Adaptive 10m Trader started ({cfg.mode}): Window 1 gathering 10m baseline (no trades). "
            f"Continuous 10m re-learning with 1hr memory archival enabled."
        )
        self.active = True
        self._publish()

    def _stake_text(self) -> str:
        c = self.cfg
        return f"{c.risk_percent:g}% of balance (max {c.max_stake:g})" if c.risk_mode == "percent" else f"{c.stake:g}"

    def _stake_now(self) -> float:
        c = self.cfg
        stake = c.stake
        bal = getattr(self.trader, "balance", None)
        if c.risk_mode == "percent" and bal:
            stake = bal * c.risk_percent / 100
        stake = max(c.min_stake, min(c.max_stake, stake))
        return round(stake, 2)

    def _halt(self, reason: str) -> None:
        self.halted = True
        self.halt_reason = reason
        self.rule = None
        self.phase = "halted"
        self.hub.log(f"{reason} Trading halted; session active.", "error")

    async def stop(self) -> None:
        if not self.active:
            return
        self.active = False
        if self._learn_task:
            self._learn_task.cancel()
        if self.trader:
            await self.trader.finish(20)
            await self.trader.stop()
        if self.cycle in self.cycles:
            self.cycles[self.cycle]["ticks"] = len(self.window_quotes)
            self.cycles[self.cycle]["partial"] = True
        self._write_cycles()
        if self._fh:
            self._fh.close()
            self._fh = None
        self.phase = "stopped"
        self.hub.log(
            f"Session finished: {self.n_trades} trades, {self.wins} won, {self.losses} lost, P/L {self.pnl:+.2f}. "
            f"Knowledge acquired over {self.kb.windows_learned} windows."
        )
        self._publish()

    # ------------------------------------------------------------------ ticks
    def on_tick(self, quote: float, epoch: int) -> tuple:
        """Returns (cycle, phase) to tag this tick's CSV row with."""
        now = time.time()
        f = self.tracker.update(quote)
        self.window_quotes.append(quote)
        self.hourly_buf.append(quote)

        if self.active:
            # 10-Minute Window Rollover & Continuous Re-learning
            if now >= self.window_end:
                self._rollover(now)

            # 1-Hour Memory Archival & Buffer Pruning
            if now >= self.next_archive:
                self._archive_and_prune(now)

        if self.trader:
            self.trader.on_tick(quote, epoch)

        if self.active:
            self._maybe_trade(f, quote, epoch)

        return self.cycle, self.phase

    def _maybe_trade(self, f, quote: float, epoch: int) -> None:
        if self.phase != "trading" or self.rule is None or self.busy or self.halted:
            return
        if self.trades_in_window >= self.cfg.max_trades_per_window:
            return
        side = self.rule.side(f)
        if side is None:
            return
        trade = Trade(
            id=self._next_id, cycle=self.cycle, mode=self.cfg.mode, symbol=self.cfg.symbol, side=side,
            expiry=self.rule.expiry, stake=self._stake_now(), rule=self.rule.name,
            reason=f"z={f.z:+.2f} run={f.run} change={f.change:+.4g}", signal_quote=quote,
            signal_epoch=epoch,
        )
        self._next_id += 1
        self.busy = True
        self.trades_in_window += 1
        self.hub.log(f"Signal: {'RISE' if side == 'CALL' else 'FALL'} {trade.expiry}t at {quote} ({trade.rule}, {trade.reason})")
        self.hub.publish("trade", trade.to_dict())
        asyncio.create_task(self._execute(trade))

    async def _execute(self, trade: Trade) -> None:
        try:
            await self.trader.open(trade)
        except Exception as e:  # noqa: BLE001
            trade.result, trade.note = "void", f"order failed: {e}"
            self._settled(trade)

    # ------------------------------------------------------------------ windows & learning
    def _new_cycle(self, n: int, phase: str) -> dict:
        return {"cycle": n, "phase": phase, "ticks": 0, "rule_used": None, "trades": 0, "wins": 0,
                "losses": 0, "pnl": 0.0, "learned": None, "decision": None, "partial": False}

    def _rollover(self, now: float) -> None:
        finished = self.cycle
        quotes = list(self.window_quotes)
        self.cycles[finished]["ticks"] = len(quotes)
        self.cycle += 1
        w = self.cfg.window_seconds
        self.window_end = self.window_end + w if now - self.window_end < w else now + w
        self.window_quotes = []
        self.trades_in_window = 0

        if self.halted:
            self.phase = "halted"
        else:
            self.phase = "analyzing"
        self.cycles[self.cycle] = self._new_cycle(self.cycle, "trade")

        # Reset in-memory frontend ticks so chart/table display only this active window
        self.hub.ticks.clear()
        self.hub.publish("window_reset", {
            "cycle": self.cycle,
            "finished": finished,
            "symbol": self.cfg.symbol,
            "kb": self.kb.to_dict(),
        })

        self.hub.log(
            f"Window {finished} complete ({len(quotes)} ticks). Updating cumulative knowledge for Window {self.cycle}..."
        )
        self._publish()
        self._learn_task = asyncio.create_task(self._learn(quotes, finished))

    def _archive_and_prune(self, now: float) -> None:
        """1-Hour memory maintenance: flushes CSV files, prunes raw tick queues to protect RAM,
        while strictly preserving the cumulative KnowledgeBase."""
        self.archive_count += 1
        self._write_cycles()
        if self._fh:
            try:
                self._fh.flush()
            except Exception:
                pass

        # Prune raw ticks buffer older than 1 hour, leaving trailing 100 ticks for tracker continuity
        if len(self.hourly_buf) > 100:
            keep = list(self.hourly_buf)[-100:]
            self.hourly_buf.clear()
            self.hourly_buf.extend(keep)

        self.next_archive = now + self.cfg.archive_seconds
        self.hub.log(
            f"1-Hour Memory Maintenance (#{self.archive_count}): Pruned raw in-memory tick queues to preserve server resources. "
            f"Cumulative knowledge preserved across {len(self.kb.rules)} rules ({self.kb.windows_learned} windows learned)."
        )
        self._publish()

    def _payout(self) -> float:
        live = getattr(self.trader, "payout_ratio", None)
        return live if live else self.cfg.payout_ratio

    async def _learn(self, quotes: list[float], finished: int) -> None:
        cfg = self.cfg
        try:
            sel = await asyncio.to_thread(
                select_strategy, quotes, self._payout(), cfg.min_trades, cfg.edge_margin, cfg.strictness, NULL_SHUFFLES,
                kb=self.kb
            )
        except Exception as e:  # noqa: BLE001
            self.hub.log(f"Strategy update failed: {e!r}", "error")
            sel = None

        if not self.active:
            return

        info = sel.to_dict() if sel else {"trade": False, "reason": "analysis failed", "top": []}
        info["learned_from"] = finished
        info["for_cycle"] = self.cycle
        self.strategy = info

        done = self.cycles[finished]
        done["learned"] = info.get("chosen") or info.get("best")
        done["decision"] = info["reason"]
        done["breakeven"] = info.get("breakeven")
        done["luck_p90"] = info.get("luck_p90")
        self.cycles[self.cycle]["decision"] = info["reason"]

        if sel and sel.chosen and not self.halted:
            self.rule = sel.chosen.rule
            self.phase = "trading"
            self.cycles[self.cycle]["rule_used"] = self.rule.name
        elif not self.halted:
            self.rule = None
            self.phase = "sitting_out"

        self.hub.log(info["reason"], "info" if info.get("trade") else "warn")
        self._write_cycles()
        self.hub.publish("strategy", info)
        self.hub.publish("cycle", self.cycles[finished])
        self.hub.publish("cycle", self.cycles[self.cycle])
        self._publish()

    # ------------------------------------------------------------------ settlement
    def _settled(self, t: Trade) -> None:
        if t._done:
            return
        t._done = True
        self.busy = False
        c = self.cycles.get(t.cycle)
        if t.result in ("won", "lost"):
            self.n_trades += 1
            self.pnl = round(self.pnl + t.profit, 2)
            if t.result == "won":
                self.wins += 1
            else:
                self.losses += 1
            if c:
                c["trades"] += 1
                c["wins"] += t.result == "won"
                c["losses"] += t.result == "lost"
                c["pnl"] = round(c["pnl"] + t.profit, 2)
        elif t.result == "unknown":
            self.unknown += 1

        if self._writer:
            self._writer.writerow([
                t.id, t.cycle, t.opened_at, t.settled_at, t.symbol, t.mode, t.side, t.expiry, t.stake, t.rule,
                t.reason, t.signal_quote, t.entry_spot if t.entry_spot is not None else "",
                t.exit_spot if t.exit_spot is not None else "", t.result, t.profit,
                t.contract_id if t.contract_id is not None else "", t.note,
            ])
            self._fh.flush()

        self.trades.append(t.to_dict())
        del self.trades[:-300]
        sign = "+" if t.profit >= 0 else ""
        self.hub.log(
            f"Trade #{t.id} {t.result.upper()} ({'RISE' if t.side == 'CALL' else 'FALL'} {t.expiry}t, "
            f"{t.entry_spot} -> {t.exit_spot}) {sign}{t.profit}. Session P/L {self.pnl:+.2f}",
            "info" if t.result in ("won", "lost") else "warn",
        )
        self.hub.publish("trade", t.to_dict())
        if c:
            self.hub.publish("cycle", c)

        if t.result == "lost":
            self.consec_losses += 1
        elif t.result == "won":
            self.consec_losses = 0

        cfg = self.cfg
        if not self.halted:
            if self.pnl <= -abs(cfg.max_session_loss):
                self._halt(f"Session loss limit reached ({self.pnl:+.2f}).")
            elif cfg.profit_target > 0 and self.pnl >= cfg.profit_target:
                self._halt(f"Profit target reached ({self.pnl:+.2f}).")
            elif cfg.max_consecutive_losses and self.consec_losses >= cfg.max_consecutive_losses:
                self._halt(f"{self.consec_losses} losses in a row.")
        self._publish()

    # ------------------------------------------------------------------ reporting
    def _write_cycles(self) -> None:
        try:
            with open(self.cycles_path, "w", newline="", encoding="utf-8") as fh:
                w = csv.writer(fh)
                w.writerow(CYCLE_COLUMNS)
                for n in sorted(self.cycles):
                    c = self.cycles[n]
                    L = c.get("learned") or {}
                    w.writerow([
                        n, c["phase"], c["ticks"], c.get("rule_used") or "", c["trades"], c["wins"], c["losses"],
                        c["pnl"], L.get("rule", ""), L.get("win_rate", ""), L.get("n", ""),
                        c.get("breakeven") or "", c.get("luck_p90") or "", c.get("decision") or "",
                    ])
        except Exception as e:  # noqa: BLE001
            self.hub.log(f"Could not write cycles file: {e!r}", "warn")

    def status(self) -> dict:
        return {
            "mode": self.cfg.mode,
            "active": self.active,
            "phase": self.phase,
            "cycle": self.cycle,
            "window_ticks": len(self.window_quotes),
            "window_end": self.window_end,
            "window_seconds": self.cfg.window_seconds,
            "next_archive": self.next_archive,
            "archive_seconds": self.cfg.archive_seconds,
            "server_now": time.time(),
            "rule": self.rule.name if self.rule else None,
            "busy": self.busy,
            "halted": self.halted,
            "halt_reason": self.halt_reason,
            "risk": self._stake_text(),
            "pnl": self.pnl,
            "trades": self.n_trades,
            "wins": self.wins,
            "losses": self.losses,
            "unknown": self.unknown,
            "stake": self._stake_now() if self.trader else self.cfg.stake,
            "strictness": self.cfg.strictness,
            "max_session_loss": self.cfg.max_session_loss,
            "payout": self._payout(),
            "kb": self.kb.to_dict(),
            "account": self.trader.account_info() if isinstance(self.trader, DerivTrader) else None,
            "files": {"trades": self.trades_path.name, "cycles": self.cycles_path.name},
        }

    def snapshot(self) -> dict:
        return {
            "status": self.status(),
            "trades": self.trades[-100:],
            "cycles": [self.cycles[k] for k in sorted(self.cycles)],
            "strategy": self.strategy,
            "kb": self.kb.to_dict(),
        }

    def _publish(self) -> None:
        self.hub.publish("session", self.status())
