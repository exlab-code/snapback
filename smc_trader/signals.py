"""Signal generation — pure functions, no side effects.

Calculates RSI, SMA, and the composite entry signal for the
2-period RSI mean-reversion strategy.
"""

from __future__ import annotations

from typing import List

import pandas as pd

from smc_trader.config import Config


def calculate_rsi(series: pd.Series, period: int) -> pd.Series:
    """Compute Wilder-smoothed RSI over *period* for a price *series*.

    Uses the exponential-moving-average method (Wilder smoothing)
    which is standard for RSI.
    """
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi


def calculate_sma(series: pd.Series, period: int) -> pd.Series:
    """Simple moving average of *series* over *period* bars."""
    return series.rolling(window=period, min_periods=period).mean()


def get_signals(
    ohlcv: pd.DataFrame,
    ticker: str,
    config: Config,
) -> pd.DataFrame:
    """Score a single ticker and return a row-per-bar DataFrame.

    Returned columns: ``date, ticker, close, rsi, sma200, volume, signal``.
    ``signal`` is ``True`` on bars where all entry conditions are met.
    """
    df = ohlcv.copy()
    df["rsi"] = calculate_rsi(df["Close"], config.rsi_period)
    df["sma200"] = calculate_sma(df["Close"], config.sma_period)
    df["avg_volume"] = df["Volume"].rolling(window=20, min_periods=20).mean()

    df["signal"] = (
        (df["Close"] > df["sma200"])
        & (df["rsi"] < config.rsi_entry)
        & (df["avg_volume"] > config.min_volume)
        & (df["Close"] > config.min_price)
    )

    result = pd.DataFrame(
        {
            "date": df.index,
            "ticker": ticker,
            "close": df["Close"].values,
            "rsi": df["rsi"].values,
            "sma200": df["sma200"].values,
            "volume": df["Volume"].values,
            "signal": df["signal"].values,
        }
    )
    return result


def scan_universe(
    data: dict[str, pd.DataFrame],
    config: Config,
) -> pd.DataFrame:
    """Run signal detection across every ticker in *data*.

    Returns a single concatenated DataFrame of all tickers, sorted by date.
    """
    frames: List[pd.DataFrame] = []
    for ticker, ohlcv in data.items():
        sig = get_signals(ohlcv, ticker, config)
        frames.append(sig)
    if not frames:
        return pd.DataFrame(
            columns=["date", "ticker", "close", "rsi", "sma200", "volume", "signal"]
        )
    return pd.concat(frames, ignore_index=True).sort_values("date")


if __name__ == "__main__":
    from smc_trader.config import Config
    from smc_trader.data import fetch_ticker
    from smc_trader.logger import setup_logging

    setup_logging()
    cfg = Config(universe=["AAPL"])
    df = fetch_ticker("AAPL", start="2023-01-01")
    if df is not None:
        signals = get_signals(df, "AAPL", cfg)
        hits = signals[signals["signal"]]
        print(f"AAPL — {len(signals)} bars, {len(hits)} entry signals")
        if not hits.empty:
            print(hits.tail(10).to_string(index=False))
    else:
        print("Could not fetch AAPL data")
