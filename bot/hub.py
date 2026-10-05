"""Shared event hub (logs, ticks, status) fanned out to dashboards over SSE."""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import datetime, timezone

logger = logging.getLogger("collector")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_LEVELS = {"debug": logging.DEBUG, "info": logging.INFO, "warn": logging.WARNING, "error": logging.ERROR}


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class Hub:
    def __init__(self) -> None:
        self.clients: set = set()
        self.logs: deque = deque(maxlen=500)
        self.ticks: deque = deque(maxlen=1500)

    def publish(self, event: str, data: dict) -> None:
        payload = {"event": event, "data": data}
        for q in list(self.clients):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                try:
                    q.get_nowait()
                    q.put_nowait(payload)
                except Exception:
                    pass

    def log(self, message: str, level: str = "info") -> None:
        entry = {"ts": utcnow_iso(), "level": level, "message": message}
        self.logs.append(entry)
        logger.log(_LEVELS.get(level, logging.INFO), message)
        self.publish("log", entry)
