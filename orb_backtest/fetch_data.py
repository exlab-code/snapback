"""
Fetch 1-minute data from Massive.com for ORB backtest.
Run: python3 orb_backtest/fetch_data.py
Requires: MASSIVE_API_KEY env var
"""
import os
import sys
import time
import requests
import pandas as pd
from datetime import datetime

CACHE_DIR = os.path.join(os.path.dirname(__file__), "minute_cache")
BASE_URL = "https://api.massive.com"

# 30 liquid S&P 500 stocks — good ORB candidates
UNIVERSE = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AMD",
    "JPM", "BAC", "GS", "MS",
    "XOM", "CVX",
    "JNJ", "LLY", "ABBV", "MRK",
    "HD", "WMT", "COST", "MCD",
    "CAT", "HON", "BA",
    "SPY", "QQQ", "IWM",
    "AMGN", "BKNG",
]


def fetch_minute_bars(ticker: str, start: str, end: str, api_key: str) -> pd.DataFrame:
    """Fetch all 1-min bars for ticker between start and end dates."""
    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/1/minute/{start}/{end}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": api_key}
    rows = []

    while url:
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 429:
                print(f"    Rate limited — sleeping 60s ...")
                time.sleep(60)
                continue
            raise

        data = resp.json()
        if data.get("status") not in ("OK", "DELAYED"):
            break
        rows.extend(data.get("results", []))
        next_url = data.get("next_url")
        if next_url:
            url = next_url
            params = {"apiKey": api_key}
        else:
            url = None

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([{
        "datetime": pd.Timestamp(r["t"], unit="ms", tz="UTC").tz_convert("US/Eastern").tz_localize(None),
        "open": r["o"],
        "high": r["h"],
        "low": r["l"],
        "close": r["c"],
        "volume": r["v"],
    } for r in rows])
    df = df.set_index("datetime").sort_index()
    # Keep only RTH bars (9:30–15:59)
    df = df.between_time("09:30", "15:59")
    return df


def main():
    api_key = os.environ.get("MASSIVE_API_KEY", "")
    if not api_key:
        print("ERROR: set MASSIVE_API_KEY environment variable")
        sys.exit(1)

    os.makedirs(CACHE_DIR, exist_ok=True)
    start = "2023-01-01"
    end = datetime.today().strftime("%Y-%m-%d")

    print(f"Fetching 1-min data for {len(UNIVERSE)} tickers ({start} → {end}) ...")
    for ticker in UNIVERSE:
        path = os.path.join(CACHE_DIR, f"{ticker}_1min.parquet")
        if os.path.exists(path):
            print(f"  {ticker}: cached, skipping")
            continue
        try:
            df = fetch_minute_bars(ticker, start, end, api_key)
            if df.empty:
                print(f"  {ticker}: no data")
            else:
                df.to_parquet(path)
                print(f"  {ticker}: {len(df):,} bars → {path}")
        except Exception as e:
            print(f"  {ticker}: ERROR {e}")
        # Small sleep between tickers to avoid rate limiting
        time.sleep(0.1)

    print("Done.")


if __name__ == "__main__":
    main()
