"""Data fetching and Parquet caching layer.

Downloads daily OHLCV bars via Massive.com REST API (when MASSIVE_API_KEY is set)
or yfinance as fallback, and caches each ticker as a Parquet file under
``./data/cache/``.  Files older than one calendar day are re-fetched.

Massive.com API is Polygon.io-compatible:
  GET https://api.massive.com/v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd
import requests

logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.getcwd(), "data", "cache")
MASSIVE_BASE_URL = "https://api.massive.com"

_INTERVAL_MAP = {"1d": "daily", "1wk": "weekly"}
_TIMESPAN_MAP  = {"1d": "day",   "1wk": "week"}


def _cache_path(ticker: str, interval: str = "1d") -> str:
    suffix = _INTERVAL_MAP.get(interval, interval)
    return os.path.join(CACHE_DIR, f"{ticker}_{suffix}.parquet")


def _is_stale(path: str, max_age_days: int = 1) -> bool:
    if not os.path.exists(path):
        return True
    mtime = datetime.fromtimestamp(os.path.getmtime(path))
    return datetime.now() - mtime > timedelta(days=max_age_days)


# ---------------------------------------------------------------------------
# Massive.com fetch
# ---------------------------------------------------------------------------

def _fetch_massive(ticker: str, start: str, interval: str) -> pd.DataFrame | None:
    api_key = os.environ.get("MASSIVE_API_KEY", "")
    if not api_key:
        return None

    timespan = _TIMESPAN_MAP.get(interval, "day")
    end = datetime.today().strftime("%Y-%m-%d")
    url = f"{MASSIVE_BASE_URL}/v2/aggs/ticker/{ticker}/range/1/{timespan}/{start}/{end}"

    rows = []
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": api_key}

    try:
        while url:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") not in ("OK", "DELAYED"):
                logger.warning("Massive non-OK status for %s: %s", ticker, data.get("status"))
                break
            rows.extend(data.get("results", []))
            url = data.get("next_url")
            params = {"apiKey": api_key}  # next_url already has other params baked in

        if not rows:
            return None

        df = pd.DataFrame([{
            "Date":   pd.Timestamp(r["t"], unit="ms").normalize(),
            "Open":   r["o"],
            "High":   r["h"],
            "Low":    r["l"],
            "Close":  r["c"],
            "Volume": r["v"],
        } for r in rows])
        df = df.set_index("Date").sort_index()
        logger.info("Massive: %s/%s — %d bars", ticker, interval, len(df))
        return df

    except Exception as exc:
        logger.warning("Massive fetch failed for %s: %s", ticker, exc)
        return None


# ---------------------------------------------------------------------------
# yfinance fetch (fallback)
# ---------------------------------------------------------------------------

def _fetch_yfinance(ticker: str, start: str, interval: str) -> pd.DataFrame | None:
    try:
        import yfinance as yf
        obj = yf.Ticker(ticker)
        df = obj.history(start=start, interval=interval, auto_adjust=True)
        if df is None or df.empty:
            return None
        df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        df.index = pd.to_datetime(df.index).tz_localize(None)
        logger.info("yfinance: %s/%s — %d bars", ticker, interval, len(df))
        return df
    except Exception as exc:
        logger.warning("yfinance fetch failed for %s: %s", ticker, exc)
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_ticker(
    ticker: str,
    start: str = "2009-01-01",
    interval: str = "1d",
) -> pd.DataFrame | None:
    """Fetch OHLCV for *ticker*, using cache when fresh.

    Uses Massive.com when ``MASSIVE_API_KEY`` is set, falls back to yfinance.
    Returns a DataFrame with columns [Open, High, Low, Close, Volume] or None.
    """
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = _cache_path(ticker, interval)

    if not _is_stale(path):
        try:
            df = pd.read_parquet(path)
            logger.debug("Cache hit: %s/%s (%d rows)", ticker, interval, len(df))
            return df
        except Exception:
            logger.warning("Corrupt cache for %s, re-fetching", ticker)

    df = _fetch_massive(ticker, start, interval)
    if df is None or df.empty:
        df = _fetch_yfinance(ticker, start, interval)

    if df is None or df.empty:
        logger.warning("No data for %s/%s", ticker, interval)
        return None

    df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
    df.to_parquet(path)
    return df


def fetch_universe(
    tickers: List[str],
    start: str = "2009-01-01",
    pause: float = 0.05,
    interval: str = "1d",
) -> Dict[str, pd.DataFrame]:
    """Fetch data for every ticker. Returns ``{ticker: DataFrame}``."""
    result: Dict[str, pd.DataFrame] = {}
    total = len(tickers)
    source = "Massive" if os.environ.get("MASSIVE_API_KEY") else "yfinance"
    logger.info("Fetching %d tickers via %s ...", total, source)

    for i, ticker in enumerate(tickers, 1):
        if i % 50 == 0:
            logger.info("Progress: %d / %d", i, total)
        df = fetch_ticker(ticker, start=start, interval=interval)
        if df is not None and not df.empty:
            result[ticker] = df
        if pause > 0 and not os.environ.get("MASSIVE_API_KEY"):
            time.sleep(pause)  # only throttle yfinance

    logger.info("Fetched %d / %d tickers", len(result), total)
    return result
