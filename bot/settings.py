"""Persistent user settings (keys, risk, strategy) saved as JSON.

The Deriv token is stored here when entered on the settings page. It is only
ever read by the server and is never sent back to the browser (the API returns
a masked hint). Environment variables are the fallback when a value is empty.
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any

# name -> (default, kind, min, max). kind: float | int | str | bool | choice
SCHEMA: dict = {
    "app_id": ("", "str", None, None),
    "account_id": ("", "str", None, None),
    "account_mode": ("demo", "choice:demo,real", None, None),
    "allow_real": (False, "bool", None, None),
    "risk_mode": ("fixed", "choice:fixed,percent", None, None),
    "stake": (1.0, "float", 0.35, None),
    "risk_percent": (1.0, "float", 0.1, 10.0),
    "max_stake": (10.0, "float", 0.35, 1000.0),
    "max_session_loss": (10.0, "float", 0.35, None),
    "profit_target": (0.0, "float", 0.0, None),          # 0 = off
    "max_consecutive_losses": (0, "int", 0, 100),        # 0 = off
    "max_trades_per_window": (20, "int", 1, 500),
    "window_minutes": (10.0, "float", 1.0, 60.0),        # 10-minute continuous learning cycle
    "archive_interval_minutes": (60.0, "float", 10.0, 1440.0),  # 1-hour periodic memory maintenance
    "strictness": ("strict", "choice:strict,normal,always", None, None),
    "min_trades": (8, "int", 3, 500),
    "edge_margin": (2.0, "float", 0.0, 20.0),            # percent
    "payout_ratio": (0.95, "float", 0.5, 1.5),
    "default_symbol": ("R_100", "str", None, None),
}
SECRET = "token"


class SettingsError(ValueError):
    pass


class Settings:
    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        self.data: dict = {k: v[0] for k, v in SCHEMA.items()}
        self.token = ""
        self._load()

    def _load(self) -> None:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return
        self.token = str(raw.pop(SECRET, "") or "")
        for k, v in raw.items():
            if k in SCHEMA:
                try:
                    self.data[k] = self._coerce(k, v)
                except SettingsError:
                    pass

    @staticmethod
    def _coerce(key: str, value: Any) -> Any:
        default, kind, lo, hi = SCHEMA[key]
        if kind == "bool":
            if isinstance(value, bool):
                return value
            raise SettingsError(f"{key} must be true or false")
        if kind.startswith("choice:"):
            if value not in kind[7:].split(","):
                raise SettingsError(f"{key} must be one of {kind[7:]}")
            return value
        if kind == "str":
            return str(value).strip()[:200]
        try:
            num = float(value) if kind == "float" else int(value)
        except (TypeError, ValueError):
            raise SettingsError(f"{key} must be a number")
        if isinstance(value, bool) or num != num:
            raise SettingsError(f"{key} must be a number")
        if lo is not None and num < lo:
            raise SettingsError(f"{key} must be at least {lo:g}")
        if hi is not None and num > hi:
            raise SettingsError(f"{key} must be at most {hi:g}")
        return num

    def update(self, patch: dict, clear_token: bool = False) -> None:
        with self._lock:
            new = dict(self.data)
            patch = dict(patch)
            tok = patch.pop(SECRET, None)
            for k, v in patch.items():
                if k not in SCHEMA:
                    continue
                if v is None:
                    continue
                new[k] = self._coerce(k, v)
            if new["stake"] > new["max_stake"]:
                raise SettingsError("stake cannot be above the maximum stake")
            self.data = new
            if clear_token:
                self.token = ""
            elif isinstance(tok, str) and tok.strip():
                t = tok.strip()
                if len(t) < 8 or len(t) > 300 or any(c.isspace() for c in t):
                    raise SettingsError("that does not look like a token")
                self.token = t
            self._save()

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(self.data)
        payload[SECRET] = self.token
        tmp = self.path.with_suffix(".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        os.replace(tmp, self.path)
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass
