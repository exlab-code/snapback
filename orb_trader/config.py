"""Configuration dataclass for the ORB trading system.

All fields have sensible defaults and can be overridden via environment
variables in ``__post_init__``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class ORBConfig:
    # Opening range parameters
    or_minutes: int = 15            # opening range duration (9:30 to 9:45)
    or_range_minutes: int = 15      # alias

    # Position limits
    max_positions: int = 3
    risk_per_trade: float = 0.015
    max_position_pct: float = 0.40

    # Stop-loss: 0 = use OR opposite side as stop
    stop_loss_pct: float = 0.0

    # Account: 0 = use actual broker equity
    account_size: float = 0.0

    # Trade window
    trade_end_hour: int = 11
    trade_end_minute: int = 0
    eod_exit_hour: int = 15
    eod_exit_minute: int = 45

    # Volume / breakout confirmation
    vol_confirm_mult: float = 1.3   # breakout bar volume must be >= mult * avg
    min_rvol: float = 1.5           # pre-market relative volume filter

    # Gap filters
    min_gap_pct: float = 1.0        # minimum gap % for scanner candidates
    max_gap_pct: float = 10.0       # maximum gap % (avoid extreme gaps)

    # Price filters
    min_price: float = 5.0
    max_price: float = 2000.0

    # ATR filters
    min_atr_pct: float = 2.0
    max_atr_pct: float = 10.0

    # Scanner
    scanner_top_n: int = 10         # take top N scored candidates

    # IBKR connection
    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = 4004
    ibkr_client_id: int = 2         # different from RSI bot (clientId=1)

    # Telegram alerts
    telegram_token: str = ""
    telegram_chat_id: str = ""

    def __post_init__(self) -> None:
        """Apply environment variable overrides."""
        if host := os.getenv("IBKR_HOST"):
            self.ibkr_host = host
        if port := os.getenv("IBKR_PORT"):
            self.ibkr_port = int(port)
        if client_id := os.getenv("ORB_CLIENT_ID"):
            self.ibkr_client_id = int(client_id)
        if account_size := os.getenv("ORB_ACCOUNT_SIZE"):
            self.account_size = float(account_size)
        if max_positions := os.getenv("ORB_MAX_POSITIONS"):
            self.max_positions = int(max_positions)
        if token := os.getenv("TELEGRAM_TOKEN"):
            self.telegram_token = token
        if chat_id := os.getenv("TELEGRAM_CHAT_ID"):
            self.telegram_chat_id = chat_id
