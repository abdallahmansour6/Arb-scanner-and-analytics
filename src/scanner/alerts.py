"""Pushover transport. Reuses the engine's existing Pushover credentials
from environment variables; no shared in-process state with the engine."""
from __future__ import annotations

import logging
import os

import httpx

log = logging.getLogger(__name__)
_PUSHOVER_URL = "https://api.pushover.net/1/messages.json"


def push(title: str, message: str, *, priority: int = 0) -> bool:
    user = os.environ.get("PUSHOVER_USER_KEY")
    token = os.environ.get("PUSHOVER_APP_TOKEN")
    if not user or not token:
        log.warning("alerts.skip reason=missing_pushover_credentials")
        return False
    try:
        r = httpx.post(
            _PUSHOVER_URL,
            data={"token": token, "user": user, "title": title,
                  "message": message, "priority": priority},
            timeout=5.0,
        )
        r.raise_for_status()
        return True
    except Exception as e:
        log.warning("alerts.error err=%s", e)
        return False
