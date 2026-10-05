"""Tick strategy engine.

Adapted from the "Universal Tick Strategy Engine" idea: rolling volatility
(z-score of each tick change) plus streak detection, normalised per asset so
the same rules work on R_50, R_75, R_100 and the 1-second indices.

What is different from the original sketch:

* Rules are parametrised (spike threshold, streak length, expiry, and whether
  to *fade* or *follow* the move) so they can be tested instead of assumed.
* ``select_strategy`` backtests every candidate rule on a recorded window with
  the exact settlement the trader uses (entry = next tick, exit = entry +
  expiry ticks, a tie is a loss) and reports *measured* win rates, not fixed
  confidence numbers.
* A "luck benchmark" repeats the same search on shuffled copies of the window.
  Searching dozens of rules always finds one that looks good in-sample, so a
  rule should beat what pure chance typically produces before it is trusted.

Pure standard library, no numpy/pandas.

CLI (walk-forward test on a recorded CSV)::

    python engine.py ticks_R_75_20261003_142436.csv --window-seconds 600
"""
from __future__ import annotations

import argparse
import csv
import math
import random
import sys
from collections import deque
from dataclasses import dataclass, asdict
from typing import Optional, Sequence

CALL = "CALL"  # Rise
PUT = "PUT"    # Fall

EXPIRIES = (2, 3, 5, 8)
SPIKE_Z = (1.5, 2.0, 2.5, 3.0)
STREAK_RUNS = (2, 3, 4, 5)


# --------------------------------------------------------------------------- features


@dataclass(frozen=True)
class Features:
    quote: float
    change: float
    z: float            # (change - mean) / std over the rolling window
    direction: int      # +1 up, -1 down, 0 flat
    run: int            # consecutive ticks in the current direction
    strong_run: int     # trailing ticks in that direction each >= 0.5 * mean |change|
    mean_abs: float
    ready: bool         # enough history for a stable baseline


class FeatureTracker:
    """Rolling baseline. Call ``update`` once per tick."""

    def __init__(self, window: int = 50, warmup: int = 20, strong_frac: float = 0.5):
        self.window = window
        self.warmup = warmup
        self.strong_frac = strong_frac
        self.prices: deque = deque(maxlen=window)
        self.changes: deque = deque(maxlen=window)
        self.run = 0
        self.direction = 0

    def update(self, quote: float, change: Optional[float] = None) -> Features:
        if change is None or (isinstance(change, float) and math.isnan(change)):
            change = round(quote - self.prices[-1], 6) if self.prices else 0.0
        self.prices.append(quote)
        self.changes.append(change)

        d = 1 if change > 0 else -1 if change < 0 else 0
        if d == 0:
            self.run, self.direction = 0, 0
        elif d == self.direction:
            self.run += 1
        else:
            self.run, self.direction = 1, d

        n = len(self.changes)
        mean = sum(self.changes) / n
        std = math.sqrt(sum((c - mean) ** 2 for c in self.changes) / n)
        mean_abs = sum(abs(c) for c in self.changes) / n
        z = (change - mean) / std if std > 0 else 0.0

        strong = 0
        if self.direction != 0:
            thr = self.strong_frac * mean_abs
            for c in reversed(self.changes):
                if c * self.direction > 0 and abs(c) >= thr:
                    strong += 1
                else:
                    break

        return Features(quote, change, z, self.direction, self.run, strong, mean_abs, n >= self.warmup)


def compute_features(quotes: Sequence[float], window: int = 50, warmup: int = 20) -> list:
    tracker = FeatureTracker(window, warmup)
    return [tracker.update(q) for q in quotes]


# --------------------------------------------------------------------------- rules


@dataclass(frozen=True)
class Rule:
    kind: str       # "spike" (one outsized tick) or "streak" (same-direction run)
    param: float    # z threshold for spike, run length for streak
    follow: bool    # True: trade with the move. False: fade it (mean reversion)
    expiry: int     # contract duration in ticks

    @property
    def name(self) -> str:
        verb = "Follow" if self.follow else "Fade"
        if self.kind == "spike":
            return f"{verb} spikes >= {self.param:g}σ / {self.expiry}t"
        return f"{verb} {int(self.param)}-tick runs / {self.expiry}t"

    def side(self, f: Features) -> Optional[str]:
        """CALL / PUT if this rule fires on the tick, else None."""
        if not f.ready or f.mean_abs <= 0:
            return None  # flat market: ties would eat the stake
        if abs(f.change) < 0.15 * f.mean_abs:
            return None  # Setup 3: dead-zone protection against flat coiling tie losses
        if self.kind == "spike":
            if f.z >= self.param:
                move = 1
            elif f.z <= -self.param:
                move = -1
            else:
                return None
        else:
            if f.direction != 0 and f.strong_run >= int(self.param):
                move = f.direction
            else:
                return None
        direction = move if self.follow else -move
        return CALL if direction > 0 else PUT


def candidate_rules() -> list:
    rules = []
    for z in SPIKE_Z:
        for follow in (False, True):
            for e in EXPIRIES:
                rules.append(Rule("spike", z, follow, e))
    for run in STREAK_RUNS:
        for follow in (True, False):
            for e in EXPIRIES:
                rules.append(Rule("streak", run, follow, e))
    return rules


# The two setups from the original sketch, for reference / the compat engine.
DEFAULT_RULES = (
    Rule("spike", 2.5, False, 5),   # fade 2.5-sigma spikes, 5 ticks
    Rule("streak", 3, True, 2),     # follow 3-tick momentum, 2 ticks
)


# --------------------------------------------------------------------------- backtest


@dataclass
class RuleStats:
    rule: Rule
    n: int
    wins: int
    ties: int
    payout: float

    @property
    def win_rate(self) -> float:
        return self.wins / self.n if self.n else 0.0

    @property
    def ev(self) -> float:
        """Expected profit per 1 staked, at this payout ratio."""
        wr = self.win_rate
        return wr * self.payout - (1 - wr)

    @property
    def wilson_lb(self) -> float:
        return wilson_lower(self.wins, self.n)

    def to_dict(self) -> dict:
        return {
            "rule": self.rule.name,
            "expiry": self.rule.expiry,
            "n": self.n,
            "wins": self.wins,
            "ties": self.ties,
            "win_rate": round(self.win_rate, 4),
            "ev": round(self.ev, 4),
            "wilson_lb": round(self.wilson_lb, 4),
        }


def wilson_lower(wins: int, n: int, z: float = 1.28) -> float:
    if n == 0:
        return 0.0
    p = wins / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return (centre - margin) / denom


def settle(quotes: Sequence[float], signal_idx: int, side: str, expiry: int) -> Optional[bool]:
    """Entry = tick after the signal, exit = entry + expiry ticks. Tie loses.

    Returns None when the window ends before the contract would settle.
    """
    entry, exit_ = signal_idx + 1, signal_idx + 1 + expiry
    if exit_ >= len(quotes):
        return None
    e, x = quotes[entry], quotes[exit_]
    if x == e:
        return False
    return (x > e) == (side == CALL)


def breakeven(payout: float) -> float:
    return 1.0 / (1.0 + payout)


def backtest_rule(rule: Rule, feats: Sequence[Features], quotes: Sequence[float], payout: float) -> RuleStats:
    n = wins = ties = 0
    next_ok = 0
    total = len(quotes)
    for i, f in enumerate(feats):
        if i < next_ok:
            continue
        side = rule.side(f)
        if side is None:
            continue
        entry, exit_ = i + 1, i + 1 + rule.expiry
        if exit_ >= total:
            break
        n += 1
        e, x = quotes[entry], quotes[exit_]
        if x == e:
            ties += 1
        elif (x > e) == (side == CALL):
            wins += 1
        next_ok = exit_  # one trade at a time; a new signal may fire on the exit tick
    return RuleStats(rule, n, wins, ties, payout)


# --------------------------------------------------------------------------- cumulative knowledge


@dataclass
class RuleKnowledge:
    name: str
    lifetime_trades: int = 0
    lifetime_wins: int = 0
    lifetime_ties: int = 0
    recent_win_rate: float = 0.5
    blended_score: float = 0.5

    @property
    def lifetime_win_rate(self) -> float:
        return self.lifetime_wins / self.lifetime_trades if self.lifetime_trades > 0 else 0.5

    def update(self, recent_n: int, recent_w: int, recent_t: int) -> None:
        self.lifetime_trades += recent_n
        self.lifetime_wins += recent_w
        self.lifetime_ties += recent_t
        if recent_n > 0:
            self.recent_win_rate = recent_w / recent_n
        # Blend 60% recent window performance + 40% cumulative lifetime track record
        self.blended_score = 0.6 * self.recent_win_rate + 0.4 * self.lifetime_win_rate


class KnowledgeBase:
    """Persistent model knowledge accumulated across 10-minute cycles.
    Retained across 1-hour raw tick purges so the bot never forgets what it has learned."""

    def __init__(self) -> None:
        self.rules: dict[str, RuleKnowledge] = {}
        self.windows_learned: int = 0
        self.total_ticks_processed: int = 0
        self.mean_volatility: float = 0.0

    def update_window(self, stats: Sequence[RuleStats], quotes: Sequence[float]) -> None:
        self.windows_learned += 1
        self.total_ticks_processed += len(quotes)
        feats = compute_features(quotes)
        if feats:
            vol = sum(f.mean_abs for f in feats) / len(feats)
            self.mean_volatility = (0.7 * self.mean_volatility + 0.3 * vol) if self.mean_volatility > 0 else vol

        for s in stats:
            rn = s.rule.name
            if rn not in self.rules:
                self.rules[rn] = RuleKnowledge(rn)
            self.rules[rn].update(s.n, s.wins, s.ties)

    def to_dict(self) -> dict:
        top_rules = sorted(self.rules.values(), key=lambda r: (r.blended_score, r.lifetime_trades), reverse=True)[:5]
        return {
            "windows_learned": self.windows_learned,
            "total_ticks_processed": self.total_ticks_processed,
            "mean_volatility": round(self.mean_volatility, 6),
            "rules_tracked": len(self.rules),
            "top_rules": [
                {
                    "name": r.name,
                    "lifetime_trades": r.lifetime_trades,
                    "lifetime_wr": round(r.lifetime_win_rate, 4),
                    "recent_wr": round(r.recent_win_rate, 4),
                    "blended_score": round(r.blended_score, 4),
                }
                for r in top_rules
            ],
        }


# --------------------------------------------------------------------------- selection


@dataclass
class Selection:
    chosen: Optional[RuleStats]
    best: Optional[RuleStats]
    top: list
    breakeven: float
    luck_p90: Optional[float]
    n_ticks: int
    n_rules: int
    strictness: str
    reason: str
    knowledge: Optional[dict] = None

    def to_dict(self) -> dict:
        return {
            "trade": self.chosen is not None,
            "chosen": self.chosen.to_dict() if self.chosen else None,
            "best": self.best.to_dict() if self.best else None,
            "top": [s.to_dict() for s in self.top],
            "breakeven": round(self.breakeven, 4),
            "luck_p90": None if self.luck_p90 is None else round(self.luck_p90, 4),
            "n_ticks": self.n_ticks,
            "n_rules": self.n_rules,
            "strictness": self.strictness,
            "reason": self.reason,
            "knowledge": self.knowledge,
        }


def _rank(stats: Sequence[RuleStats], min_trades: int, kb: Optional[KnowledgeBase] = None) -> list:
    valid = [s for s in stats if s.n >= min_trades]
    if kb and kb.windows_learned > 0:
        def score(s: RuleStats):
            rk = kb.rules.get(s.rule.name)
            lifetime_wr = rk.lifetime_win_rate if rk else 0.5
            blended = 0.6 * s.wilson_lb + 0.4 * lifetime_wr
            return (blended, s.n)
        valid.sort(key=score, reverse=True)
    else:
        valid.sort(key=lambda s: (s.wilson_lb, s.n), reverse=True)
    return valid


def luck_benchmark(quotes: Sequence[float], payout: float, min_trades: int,
                   shuffles: int = 30, seed: Optional[int] = None) -> Optional[float]:
    """90th percentile of the win rate the *same selection* gets on shuffled data.

    Shuffling the tick changes keeps their size distribution but destroys any
    sequence structure, so whatever the search finds there is luck.
    """
    if shuffles <= 0 or len(quotes) < 3:
        return None
    rng = random.Random(seed)
    changes = [quotes[i] - quotes[i - 1] for i in range(1, len(quotes))]
    rules = candidate_rules()
    results = []
    for _ in range(shuffles):
        rng.shuffle(changes)
        q, shuffled = quotes[0], [quotes[0]]
        for c in changes:
            q = round(q + c, 6)
            shuffled.append(q)
        feats = compute_features(shuffled)
        ranked = _rank([backtest_rule(r, feats, shuffled, payout) for r in rules], min_trades)
        if ranked:
            results.append(ranked[0].win_rate)
    if not results:
        return None
    results.sort()
    return results[min(len(results) - 1, int(0.9 * len(results)))]


def select_strategy(quotes: Sequence[float], payout: float = 0.95, min_trades: int = 12,
                    edge_margin: float = 0.02, strictness: str = "normal",
                    null_shuffles: int = 30, seed: Optional[int] = None,
                    kb: Optional[KnowledgeBase] = None) -> Selection:
    """Pick the rule to trade next window from one recorded window.

    strictness:
      "always"  trade the best rule, whatever its numbers
      "normal"  need win rate >= breakeven + edge_margin and min_trades trades
      "strict"  as normal, and also beat the luck benchmark
    """
    be = breakeven(payout)
    rules = candidate_rules()
    if len(quotes) < 60:
        return Selection(None, None, [], be, None, len(quotes), len(rules), strictness,
                         f"Only {len(quotes)} ticks recorded, need at least 60.",
                         knowledge=kb.to_dict() if kb else None)

    feats = compute_features(quotes)
    stats = [backtest_rule(r, feats, quotes, payout) for r in rules]

    if kb:
        kb.update_window(stats, quotes)

    ranked = _rank(stats, min_trades, kb)
    top = ranked[:8]
    if not ranked:
        return Selection(None, None, [], be, None, len(quotes), len(rules), strictness,
                         f"No rule fired {min_trades}+ times in this window.",
                         knowledge=kb.to_dict() if kb else None)

    best = ranked[0]
    luck = luck_benchmark(quotes, payout, min_trades, null_shuffles, seed) if (strictness == "strict" or null_shuffles) else None

    chosen, why = None, ""
    wr_txt = f"{best.win_rate:.0%} of {best.n}"
    rk = kb.rules.get(best.rule.name) if kb else None
    blend_txt = f", lifetime {rk.lifetime_win_rate:.0%} ({rk.lifetime_trades}t)" if rk and rk.lifetime_trades > best.n else ""
    if strictness == "always":
        chosen = best
        why = f"Trading best rule {best.rule.name}: window {wr_txt}{blend_txt} (breakeven {be:.0%})."
    elif best.win_rate < be + edge_margin:
        why = f"Sitting out: best rule {best.rule.name} won {wr_txt}{blend_txt}, below breakeven {be:.0%} + {edge_margin:.0%} margin."
    elif luck is not None and best.win_rate <= luck:
        lk = f"{luck:.0%}"
        why = f"Sitting out: best rule {best.rule.name} won {wr_txt}{blend_txt}, which does not beat luck benchmark ({lk})."
    else:
        chosen = best
        lk = "" if luck is None else f", luck benchmark {luck:.0%}"
        why = f"Trading {best.rule.name}: window {wr_txt}{blend_txt} (breakeven {be:.0%}{lk})."

    return Selection(chosen, best, top, be, luck, len(quotes), len(rules), strictness, why,
                     knowledge=kb.to_dict() if kb else None)


# --------------------------------------------------------------------------- compat engine


@dataclass
class TradeSignal:
    action: str          # BUY_RISE / BUY_FALL / HOLD
    contract_type: str   # RISE_FALL or NONE
    expiry_ticks: int
    entry_price: float
    confidence: float    # measured win rate if known, else 0.5
    reason: str


class UniversalTickStrategyEngine:
    """Same call pattern as the original sketch: ``ingest_tick`` per tick.

    Uses the two original setups by default, or any list of ``Rule`` objects
    (for example the one ``select_strategy`` chose).
    """

    def __init__(self, window_size: int = 50, rules: Optional[Sequence[Rule]] = None,
                 win_rates: Optional[dict] = None):
        self.tracker = FeatureTracker(window_size)
        self.rules = list(rules or DEFAULT_RULES)
        self.win_rates = win_rates or {}

    def ingest_tick(self, quote: float, change: Optional[float] = None) -> TradeSignal:
        f = self.tracker.update(quote, change)
        if not f.ready:
            return TradeSignal("HOLD", "NONE", 0, quote, 0.0, "Collecting baseline data...")
        for rule in self.rules:
            side = rule.side(f)
            if side:
                return TradeSignal(
                    "BUY_RISE" if side == CALL else "BUY_FALL", "RISE_FALL", rule.expiry, quote,
                    self.win_rates.get(rule.name, 0.5),
                    f"{rule.name}: z={f.z:+.2f}, run={f.run}, change={f.change:+.4g}",
                )
        return TradeSignal("HOLD", "NONE", 0, quote, 0.5, "No setup.")


# --------------------------------------------------------------------------- CLI


def _load_csv(path: str):
    rows = []
    with open(path, newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            try:
                rows.append((int(float(r["epoch"])), float(r["quote"])))
            except (KeyError, ValueError):
                continue
    return rows


def walk_forward(rows, window_seconds: int, payout: float, min_trades: int, edge_margin: float,
                 strictness: str, shuffles: int) -> None:
    if not rows:
        print("No ticks found in file.")
        return
    t0 = rows[0][0]
    windows: dict = {}
    for epoch, q in rows:
        windows.setdefault((epoch - t0) // window_seconds, []).append(q)
    keys = sorted(windows)
    print(f"{len(rows)} ticks in {len(keys)} window(s) of {window_seconds}s. payout={payout}, breakeven={breakeven(payout):.1%}\n")
    total_n = total_w = 0
    for a, b in zip(keys, keys[1:]):
        if b != a + 1:
            continue
        sel = select_strategy(windows[a], payout, min_trades, edge_margin, strictness, shuffles, seed=a)
        line = f"window {a}->{b}: {sel.reason}"
        if sel.chosen:
            test = backtest_rule(sel.chosen.rule, compute_features(windows[b]), windows[b], payout)
            total_n += test.n
            total_w += test.wins
            line += f"\n    out-of-sample: {test.wins}/{test.n} won" + (f" ({test.win_rate:.0%})" if test.n else "")
        print(line)
    if total_n:
        wr = total_w / total_n
        pnl = total_w * payout - (total_n - total_w)
        print(f"\nOut-of-sample total: {total_w}/{total_n} won ({wr:.1%}) vs breakeven {breakeven(payout):.1%}; P/L {pnl:+.2f} per 1 staked")
    else:
        print("\nNo out-of-sample trades (no window passed selection).")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Walk-forward test of the tick strategy engine on a recorded CSV.")
    ap.add_argument("csv")
    ap.add_argument("--window-seconds", type=int, default=600)
    ap.add_argument("--payout", type=float, default=0.95)
    ap.add_argument("--min-trades", type=int, default=12)
    ap.add_argument("--edge-margin", type=float, default=0.02)
    ap.add_argument("--strictness", choices=["strict", "normal", "always"], default="normal")
    ap.add_argument("--shuffles", type=int, default=30)
    a = ap.parse_args(argv)
    walk_forward(_load_csv(a.csv), a.window_seconds, a.payout, a.min_trades, a.edge_margin, a.strictness, a.shuffles)
    return 0


if __name__ == "__main__":
    sys.exit(main())
