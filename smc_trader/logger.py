"""Structured logging and optional Telegram alerts.

Sets up a RotatingFileHandler (10 MB, 10 backups) writing to ./logs/trader.log
and a console handler at INFO level.  The ``send_telegram`` helper fires only
when both ``telegram_token`` and ``telegram_chat_id`` are configured; failures
are logged but never raised.
"""

from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Optional

import requests

_CONFIGURED = False

LOG_DIR = os.path.join(os.getcwd(), "logs")
LOG_FILE = os.path.join(LOG_DIR, "trader.log")


def setup_logging() -> None:
    """Initialise root logger with file + console handlers (idempotent)."""
    global _CONFIGURED
    if _CONFIGURED:
        return
    _CONFIGURED = True

    os.makedirs(LOG_DIR, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Rotating file handler — DEBUG+
    fh = RotatingFileHandler(
        LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=10
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    # Console handler — INFO+
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    root.addHandler(ch)


def send_telegram(
    message: str,
    token: str = "",
    chat_id: str = "",
) -> bool:
    """Send a Telegram message.  Returns True on success, False otherwise.

    If *token* or *chat_id* are empty the call is silently skipped.
    Network / API errors are logged but never re-raised.
    """
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        resp.raise_for_status()
        return True
    except Exception as exc:
        logging.getLogger(__name__).warning("Telegram send failed: %s", exc)
        return False


def alert(
    message: str,
    token: str = "",
    chat_id: str = "",
    level: int = logging.INFO,
) -> None:
    """Log *message* and optionally forward it to Telegram."""
    logging.getLogger("smc_trader.alert").log(level, message)
    send_telegram(message, token=token, chat_id=chat_id)


if __name__ == "__main__":
    setup_logging()
    log = logging.getLogger("self_test")
    log.debug("debug message")
    log.info("info message")
    log.warning("warning message")
    print(f"Log file: {LOG_FILE}")
    print("Telegram (no creds): ", send_telegram("test"))
