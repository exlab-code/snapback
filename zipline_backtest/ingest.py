"""Convert yfinance Parquet cache to Zipline csvdir bundle format.

Run once before ingesting:
    python3 zipline_backtest/ingest.py

Then ingest:
    zipline ingest -b smc_csvdir
"""

from __future__ import annotations

import os
import glob
import pandas as pd

CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "cache")
CSV_DIR   = os.path.join(os.path.dirname(__file__), "csv_bundle")
DAILY_DIR = os.path.join(CSV_DIR, "daily")


def convert() -> None:
    os.makedirs(DAILY_DIR, exist_ok=True)
    files = glob.glob(os.path.join(CACHE_DIR, "*_daily.parquet"))
    print(f"Converting {len(files)} Parquet files → Zipline CSVs ...")

    for path in files:
        ticker = os.path.basename(path).replace("_daily.parquet", "")
        df = pd.read_parquet(path)
        df.index = pd.to_datetime(df.index).tz_localize(None)
        df = df.rename(columns={
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        })
        df["dividend"] = 0.0
        df["split"] = 1.0
        df = df[["open", "high", "low", "close", "volume", "dividend", "split"]]
        df = df.loc[df.index >= "2003-01-01"]
        out = os.path.join(DAILY_DIR, f"{ticker}.csv")
        df.to_csv(out, index_label="date")
        print(f"  {ticker}: {len(df)} bars → {out}")

    print(f"\nDone. CSV files in: {DAILY_DIR}")
    print("Next: zipline ingest -b smc_csvdir")


if __name__ == "__main__":
    convert()
