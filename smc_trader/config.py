"""Configuration for the SMC swing trading system.

All tunable parameters live here. No other module should hardcode
trading parameters — import Config and read from it instead.
"""

from __future__ import annotations

import logging
import ssl
from dataclasses import dataclass, field
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)


def _fetch_sp500_tickers() -> List[str]:
    """Fetch current S&P 500 constituents from Wikipedia."""
    try:
        # Work around missing local SSL certificates
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        import urllib.request
        opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx))
        opener.addheaders = [("User-Agent", "Mozilla/5.0 (smc_trader)")]
        urllib.request.install_opener(opener)
        table = pd.read_html(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            attrs={"id": "constituents"},
        )[0]
        tickers = sorted(table["Symbol"].str.replace(".", "-", regex=False).tolist())
        logger.info("Fetched %d S&P 500 tickers from Wikipedia", len(tickers))
        return tickers
    except Exception as exc:
        logger.warning("Failed to fetch S&P 500 list (%s), using fallback", exc)
        return _FALLBACK_TICKERS


# Curated 40-stock universe: high liquidity, long history (pre-2010),
# diversified across sectors — avoids Backtrader multi-feed sync issues.
_FALLBACK_TICKERS: List[str] = [
    # Technology
    "AAPL", "MSFT", "GOOGL", "NVDA", "AVGO", "TXN", "QCOM", "CSCO",
    # Consumer Discretionary
    "AMZN", "HD", "MCD", "NKE", "TGT", "LOW",
    # Financials
    "JPM", "BAC", "WFC", "GS", "V", "MA",
    # Health Care
    "JNJ", "UNH", "LLY", "MRK", "ABT", "TMO",
    # Energy
    "XOM", "CVX",
    # Industrials
    "CAT", "HON", "UNP", "UPS",
    # Consumer Staples
    "PG", "KO", "PEP", "WMT", "COST",
    # Communication Services
    "DIS", "CMCSA", "VZ",
    # Utilities / REITs
    "NEE", "SO",
]


@dataclass
class Config:
    """Central configuration for the trading system."""

    # Universe — defaults to the curated 40-stock list.
    # Pass universe=[] and use_full_sp500=True to fetch all S&P 500 constituents.
    universe: List[str] = field(default_factory=list)
    use_full_sp500: bool = False

    # Bar interval: "1d" for daily, "1wk" for weekly
    bar_interval: str = "1d"

    # RSI parameters
    rsi_period: int = 2
    rsi_entry: float = 10.0
    rsi_exit: float = 70.0

    # Trend filter — 40 weeks ≈ 200 trading days
    sma_period: int = 200

    # Filters — weekly volume is ~5× daily, so 5M ≈ 1M/day
    min_volume: int = 1_000_000
    min_price: float = 10.0

    # Position sizing / risk
    risk_per_trade: float = 0.015
    stop_loss_pct: float = 0.05
    max_positions: int = 5
    max_position_pct: float = 0.25
    time_stop_days: int = 10
    initial_capital: float = 20_000.0

    # Override equity used for position sizing (0 = use actual broker equity)
    account_size: float = 0.0

    # IBKR connection
    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = 4002
    ibkr_client_id: int = 1

    # Massive.com API key (empty = use yfinance fallback)
    massive_api_key: str = ""

    # Telegram alerts (empty = disabled)
    telegram_token: str = ""
    telegram_chat_id: str = ""

    def __post_init__(self) -> None:
        import os
        # Environment variable overrides (for Docker / VPS deployment)
        if os.environ.get("IBKR_HOST"):
            self.ibkr_host = os.environ["IBKR_HOST"]
        if os.environ.get("IBKR_PORT"):
            self.ibkr_port = int(os.environ["IBKR_PORT"])
        if os.environ.get("IBKR_CLIENT_ID"):
            self.ibkr_client_id = int(os.environ["IBKR_CLIENT_ID"])
        if os.environ.get("ACCOUNT_SIZE"):
            self.account_size = float(os.environ["ACCOUNT_SIZE"])
        if os.environ.get("MASSIVE_API_KEY"):
            self.massive_api_key = os.environ["MASSIVE_API_KEY"]
        if os.environ.get("TELEGRAM_TOKEN"):
            self.telegram_token = os.environ["TELEGRAM_TOKEN"]
        if os.environ.get("TELEGRAM_CHAT_ID"):
            self.telegram_chat_id = os.environ["TELEGRAM_CHAT_ID"]
        if os.environ.get("USE_FULL_SP500", "").lower() == "true":
            self.use_full_sp500 = True

        if not self.universe:
            if self.use_full_sp500:
                self.universe = _fetch_sp500_tickers()
            else:
                self.universe = list(_FALLBACK_TICKERS)


if __name__ == "__main__":
    cfg = Config()
    print(f"Universe size : {len(cfg.universe)}")
    print(f"RSI period    : {cfg.rsi_period}")
    print(f"RSI entry     : {cfg.rsi_entry}")
    print(f"RSI exit      : {cfg.rsi_exit}")
    print(f"SMA period    : {cfg.sma_period}")
    print(f"Stop loss %   : {cfg.stop_loss_pct}")
    print(f"Risk/trade    : {cfg.risk_per_trade}")
    print(f"Max positions : {cfg.max_positions}")
    print(f"Capital       : ${cfg.initial_capital:,.2f}")
    print(f"First 10 tickers: {cfg.universe[:10]}")
