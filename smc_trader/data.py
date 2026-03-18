"""Data fetching and Parquet caching layer.

Downloads daily OHLCV bars via *yfinance* and caches each ticker as a
Parquet file under ``./data/cache/``.  Files older than one calendar day
are automatically re-fetched.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd
import yfinance as yf

from smc_trader.config import Config

logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.getcwd(), "data", "cache")


def _cache_path(ticker: str, interval: str = "1d") -> str:
    """Return the Parquet file path for *ticker* and *interval*."""
    suffix = interval.replace("1d", "daily").replace("1wk", "weekly")
    return os.path.join(CACHE_DIR, f"{ticker}_{suffix}.parquet")


def _is_stale(path: str, max_age_days: int = 1) -> bool:
    """Return True if *path* doesn't exist or is older than *max_age_days*."""
    if not os.path.exists(path):
        return True
    mtime = datetime.fromtimestamp(os.path.getmtime(path))
    return datetime.now() - mtime > timedelta(days=max_age_days)


def fetch_ticker(
    ticker: str,
    start: str = "2009-01-01",
    interval: str = "1d",
) -> pd.DataFrame | None:
    """Download OHLCV for *ticker* at *interval* granularity, using cache when fresh.

    *interval* is passed directly to yfinance (e.g. ``"1d"``, ``"1wk"``).
    Returns a DataFrame indexed by date with columns
    [Open, High, Low, Close, Volume], or ``None`` on failure.
    """
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = _cache_path(ticker, interval)

    if not _is_stale(path):
        try:
            df = pd.read_parquet(path)
            logger.debug("Cache hit for %s/%s (%d rows)", ticker, interval, len(df))
            return df
        except Exception:
            logger.warning("Corrupt cache for %s/%s, re-fetching", ticker, interval)

    try:
        obj = yf.Ticker(ticker)
        df = obj.history(start=start, interval=interval, auto_adjust=True)
        if df is None or df.empty:
            logger.warning("No data returned for %s/%s", ticker, interval)
            return None
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df.index = pd.to_datetime(df.index).tz_localize(None)
        df.to_parquet(path)
        logger.info("Fetched %s/%s — %d bars", ticker, interval, len(df))
        return df
    except Exception as exc:
        logger.warning("Failed to fetch %s/%s: %s", ticker, interval, exc)
        return None


def fetch_universe(
    tickers: List[str],
    start: str = "2009-01-01",
    pause: float = 0.05,
    interval: str = "1d",
) -> Dict[str, pd.DataFrame]:
    """Fetch data for every ticker in the universe.

    Returns ``{ticker: DataFrame}``; failed tickers are logged and skipped.
    """
    result: Dict[str, pd.DataFrame] = {}
    total = len(tickers)
    for i, ticker in enumerate(tickers, 1):
        if i % 50 == 0:
            logger.info("Fetching data: %d / %d", i, total)
        df = fetch_ticker(ticker, start=start, interval=interval)
        if df is not None and not df.empty:
            result[ticker] = df
        if pause > 0:
            time.sleep(pause)
    logger.info("Fetched %d / %d tickers successfully", len(result), total)
    return result


if __name__ == "__main__":
    from smc_trader.logger import setup_logging

    setup_logging()
    # Quick self-test: fetch a handful of tickers
    test_tickers = ["AAPL", "MSFT", "INVALID_TICKER_XYZ"]
    data = fetch_universe(test_tickers, start="2023-01-01", pause=0)
    for t, df in data.items():
        print(f"{t}: {len(df)} bars, last close ${df['Close'].iloc[-1]:.2f}")
    print(f"Missing: {set(test_tickers) - set(data)}")
