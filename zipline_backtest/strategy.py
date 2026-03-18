"""RSI(2) mean-reversion strategy for Zipline.

Run with:
    python3 zipline_backtest/strategy.py

Produces a pyfolio tear sheet comparing the strategy to SPY.
"""

from __future__ import annotations

import os
import sys
import warnings
warnings.filterwarnings("ignore")

# Register the bundle before importing zipline run_algorithm
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import extension  # noqa: F401 — registers smc_csvdir bundle

import numpy as np
import pandas as pd

from zipline import run_algorithm
from zipline.api import (
    attach_pipeline,
    date_rules,
    order_target_percent,
    pipeline_output,
    record,
    schedule_function,
    set_benchmark,
    set_commission,
    set_slippage,
    symbol,
    time_rules,
)
from zipline.finance.commission import PerTrade
from zipline.finance.slippage import VolumeShareSlippage
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.factors import AverageDollarVolume, RSI, SimpleMovingAverage

# ── parameters (mirrors smc_trader/config.py) ──────────────────────────────
RSI_PERIOD      = 2
RSI_ENTRY       = 10.0
RSI_EXIT        = 70.0
SMA_PERIOD      = 200
MIN_AVG_VOLUME  = 1_000_000
MIN_PRICE       = 10.0
RISK_PER_TRADE  = 0.015
STOP_PCT        = 0.05
MAX_POSITIONS   = 5
MAX_POS_PCT     = 0.25
TIME_STOP_DAYS  = 10
INITIAL_CAPITAL = 20_000.0


# ── pipeline ────────────────────────────────────────────────────────────────

def make_pipeline() -> Pipeline:
    """Compute RSI, SMA200, and entry signal for every stock in the bundle."""
    rsi    = RSI(window_length=RSI_PERIOD + 1)
    sma200 = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=SMA_PERIOD)
    price  = USEquityPricing.close.latest
    volume = USEquityPricing.volume.latest

    signal = (
        (price > sma200)
        & (rsi < RSI_ENTRY)
        & (volume > MIN_AVG_VOLUME)
        & (price > MIN_PRICE)
    )

    return Pipeline(
        columns={
            "rsi":    rsi,
            "sma200": sma200,
            "price":  price,
            "signal": signal,
        },
        screen=price > 0,
    )


# ── strategy ────────────────────────────────────────────────────────────────

def initialize(context):
    context.open_positions = {}  # symbol -> {entry_price, entry_date}
    set_commission(PerTrade(cost=1.0))
    set_slippage(VolumeShareSlippage(volume_limit=0.025, price_impact=0.001))
    attach_pipeline(make_pipeline(), "signals")
    schedule_function(
        rebalance,
        date_rules.every_day(),
        time_rules.market_open(minutes=1),
    )


def rebalance(context, data):
    pipeline = pipeline_output("signals")
    portfolio_value = context.portfolio.portfolio_value

    # ── exits ──
    for asset, meta in list(context.open_positions.items()):
        if not data.can_trade(asset):
            continue
        try:
            rsi_val    = pipeline.loc[asset, "rsi"]   if asset in pipeline.index else np.nan
            current    = data.current(asset, "price")
            days_held  = (context.datetime.date() - meta["entry_date"]).days
        except Exception:
            continue

        exit_reason = None
        if rsi_val > RSI_EXIT:
            exit_reason = "rsi"
        elif current <= meta["stop_price"]:
            exit_reason = "stop"
        elif days_held >= TIME_STOP_DAYS:
            exit_reason = "time"

        if exit_reason:
            order_target_percent(asset, 0.0)
            del context.open_positions[asset]

    # ── entries ──
    n_open = len(context.open_positions)
    if n_open >= MAX_POSITIONS:
        return

    candidates = pipeline[pipeline["signal"]].copy()
    candidates = candidates.sort_values("rsi")  # most oversold first

    for asset, row in candidates.iterrows():
        if n_open >= MAX_POSITIONS:
            break
        if asset in context.open_positions:
            continue
        if not data.can_trade(asset):
            continue

        price = row["price"]
        if price <= 0:
            continue

        risk_shares = (portfolio_value * RISK_PER_TRADE) / (price * STOP_PCT)
        cap_shares  = (portfolio_value * MAX_POS_PCT)    / price
        shares      = int(min(risk_shares, cap_shares))
        if shares <= 0:
            continue

        target_pct = (shares * price) / portfolio_value
        order_target_percent(asset, target_pct)
        context.open_positions[asset] = {
            "entry_price": price,
            "entry_date":  context.datetime.date(),
            "stop_price":  price * (1 - STOP_PCT),
        }
        n_open += 1

    record(
        positions=n_open,
        portfolio_value=portfolio_value,
    )


# ── run ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pyfolio as pf
    import matplotlib
    matplotlib.use("Agg")  # headless
    import matplotlib.pyplot as plt

    print("Running Zipline backtest 2010-01-01 → 2025-12-31 ...")

    result = run_algorithm(
        start=pd.Timestamp("2010-01-01"),
        end=pd.Timestamp("2025-12-31"),
        initialize=initialize,
        capital_base=INITIAL_CAPITAL,
        bundle="smc_csvdir",
        data_frequency="daily",
    )

    # ── summary ──────────────────────────────────────────────────────────────
    start_val = INITIAL_CAPITAL
    end_val   = result["portfolio_value"].iloc[-1]
    total_ret = (end_val / start_val - 1) * 100
    returns   = result["returns"]
    sharpe    = returns.mean() / returns.std() * (252 ** 0.5)
    max_dd    = (result["portfolio_value"] / result["portfolio_value"].cummax() - 1).min() * 100

    print()
    print("=" * 50)
    print("  ZIPLINE RESULTS")
    print("=" * 50)
    print(f"  Start capital  : ${start_val:>12,.2f}")
    print(f"  End capital    : ${end_val:>12,.2f}")
    print(f"  Total return   : {total_ret:>11.2f}%")
    print(f"  Sharpe ratio   : {sharpe:>12.3f}")
    print(f"  Max drawdown   : {max_dd:>11.2f}%")
    print("=" * 50)

    # ── pyfolio tear sheet ────────────────────────────────────────────────────
    print("\nGenerating pyfolio tear sheet → zipline_backtest/tearsheet.png ...")
    fig = pf.create_returns_tear_sheet(returns, return_fig=True)
    out = os.path.join(os.path.dirname(__file__), "tearsheet.png")
    fig.savefig(out, bbox_inches="tight", dpi=150)
    print(f"Saved: {out}")
