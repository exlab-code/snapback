"""Backtrader-based backtesting engine for the 2-period RSI mean-reversion strategy.

Feeds cached Parquet data into Backtrader, runs the ``RSIMeanReversion``
strategy, and prints a clean summary of key performance metrics.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import backtrader as bt
import pandas as pd

from smc_trader.config import Config

logger = logging.getLogger(__name__)


class FixedPerTradeCommission(bt.CommInfoBase):
    """Flat $N per order (not per share)."""

    params = (("commission", 1.0), ("stocklike", True), ("commtype", bt.CommInfoBase.COMM_FIXED))

    def _getcommission(self, size: float, price: float, pseudoexec: bool) -> float:
        return self.p.commission


# ---------------------------------------------------------------------------
# Internal trade record (Backtrader's TradeAnalyzer miscounts with close())
# ---------------------------------------------------------------------------

@dataclass
class TradeRecord:
    """One completed round-trip trade."""

    ticker: str
    entry_price: float
    exit_price: float
    shares: int
    bars_held: int
    pnl_pct: float
    exit_reason: str


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

class RSIMeanReversion(bt.Strategy):
    """2-period RSI mean-reversion strategy for Backtrader.

    Manages entries, bracket exits (stop loss + RSI exit + time stop),
    and enforces the maximum number of concurrent positions.
    """

    params: dict = dict(
        rsi_period=2,
        rsi_entry=10.0,
        rsi_exit=70.0,
        sma_period=200,
        min_volume=1_000_000,
        min_price=10.0,
        risk_per_trade=0.015,
        stop_loss_pct=0.05,
        max_positions=5,
        max_position_pct=0.25,
        time_stop_days=10,
    )

    # -- lifecycle -----------------------------------------------------------

    def __init__(self) -> None:
        self.open_positions: Dict[str, dict] = {}  # ticker -> meta
        self.pending_exit: Dict[str, str] = {}  # ticker -> exit_reason
        self.pending_close: set = set()  # tickers with an unconfirmed sell order
        self.completed_trades: List[TradeRecord] = []

        # Pre-compute indicators per data feed
        self.indicators: Dict[str, dict] = {}
        for d in self.datas:
            ticker = d._name
            self.indicators[ticker] = {
                "rsi": bt.indicators.RSI(d.close, period=self.p.rsi_period, safediv=True),
                "sma": bt.indicators.SMA(d.close, period=self.p.sma_period),
                "avg_vol": bt.indicators.SMA(d.volume, period=20),
            }

    # -- order notifications -------------------------------------------------

    def notify_order(self, order: bt.Order) -> None:
        if order.status in (order.Submitted, order.Accepted):
            return

        ticker = order.data._name if order.data else "?"

        if order.status == order.Completed:
            if order.isbuy():
                entry_price = order.executed.price
                self.open_positions[ticker] = {
                    "entry_price": entry_price,
                    "stop_price": entry_price * (1 - self.p.stop_loss_pct),
                    "entry_bar": len(self),
                    "shares": int(order.executed.size),
                }
                logger.debug(
                    "BUY filled %s @ %.2f (%d shares)",
                    ticker, entry_price, order.executed.size,
                )
            elif order.issell():
                self.pending_close.discard(ticker)
                if ticker in self.open_positions:
                    meta = self.open_positions[ticker]
                    bars_held = len(self) - meta["entry_bar"]
                    pnl_pct = (order.executed.price / meta["entry_price"] - 1) * 100
                    reason = self.pending_exit.pop(ticker, "unknown")
                    self.completed_trades.append(
                        TradeRecord(
                            ticker=ticker,
                            entry_price=meta["entry_price"],
                            exit_price=order.executed.price,
                            shares=meta["shares"],
                            bars_held=bars_held,
                            pnl_pct=pnl_pct,
                            exit_reason=reason,
                        )
                    )
                    logger.debug(
                        "SELL filled %s @ %.2f (held %d bars, pnl=%.2f%%, %s)",
                        ticker, order.executed.price, bars_held, pnl_pct, reason,
                    )
                    del self.open_positions[ticker]

        elif order.status in (order.Canceled, order.Margin, order.Rejected):
            logger.debug("Order %s for %s: %s", order.ref, ticker, order.getstatusname())
            self.pending_exit.pop(ticker, None)
            self.pending_close.discard(ticker)

    # -- main logic ----------------------------------------------------------

    def next(self) -> None:
        # --- Check exits on open positions first ---
        for d in self.datas:
            ticker = d._name
            if ticker not in self.open_positions:
                continue
            # Reconcile: if broker has no position, clean up stale entry
            if self.getposition(data=d).size == 0:
                self.open_positions.pop(ticker, None)
                self.pending_exit.pop(ticker, None)
                continue

            pos_meta = self.open_positions[ticker]
            current_close = d.close[0]
            rsi_val = self.indicators[ticker]["rsi"][0]
            bars_held = len(self) - pos_meta["entry_bar"]

            if math.isnan(rsi_val) or math.isnan(current_close):
                continue

            exit_reason: Optional[str] = None

            # RSI exit
            if rsi_val > self.p.rsi_exit:
                exit_reason = "rsi_exit"
            # Hard stop loss
            elif current_close <= pos_meta["stop_price"]:
                exit_reason = "stop_loss"
            # Time stop
            elif bars_held >= self.p.time_stop_days:
                exit_reason = "time_stop"

            if exit_reason and self.getposition(data=d).size > 0 and ticker not in self.pending_close:
                self.pending_exit[ticker] = exit_reason
                self.pending_close.add(ticker)
                self.close(data=d)
                logger.debug(
                    "%s %s — close=%.2f rsi=%.1f bars=%d",
                    exit_reason.upper(), ticker, current_close, rsi_val, bars_held,
                )

        # --- Check entries ---
        if len(self.open_positions) >= self.p.max_positions:
            return

        # Track cash reserved by orders already submitted this bar
        cash_remaining = self.broker.getcash()

        for d in self.datas:
            if len(self.open_positions) >= self.p.max_positions:
                break
            if cash_remaining <= 0:
                break

            ticker = d._name
            if ticker in self.open_positions:
                continue

            # Need enough bars for the SMA
            if len(d) < self.p.sma_period:
                continue

            ind = self.indicators[ticker]
            close = d.close[0]
            rsi_val = ind["rsi"][0]
            sma_val = ind["sma"][0]
            avg_vol = ind["avg_vol"][0]

            # Skip NaN indicator values
            if math.isnan(rsi_val) or math.isnan(sma_val) or math.isnan(avg_vol):
                continue

            # Entry conditions
            if (
                close > sma_val
                and rsi_val < self.p.rsi_entry
                and avg_vol > self.p.min_volume
                and close > self.p.min_price
            ):
                equity = self.broker.getvalue()
                if math.isnan(equity) or equity <= 0:
                    continue
                risk_shares = (equity * self.p.risk_per_trade) / (
                    close * self.p.stop_loss_pct
                )
                cap_shares = (equity * self.p.max_position_pct) / close
                raw = min(risk_shares, cap_shares)
                if math.isnan(raw) or raw <= 0:
                    continue
                shares = int(math.floor(raw))
                if shares <= 0:
                    continue
                # Never spend more than remaining cash this bar (no leverage)
                if shares * close > cash_remaining:
                    shares = int(math.floor(cash_remaining / close))
                if shares <= 0:
                    continue

                self.buy(data=d, size=shares)
                cash_remaining -= shares * close  # reserve cash for this order
                logger.debug(
                    "BUY order %s — %d shares (rsi=%.1f, sma=%.1f)",
                    ticker, shares, rsi_val, sma_val,
                )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_backtest(
    data: Dict[str, pd.DataFrame],
    config: Config,
    start: str = "2010-01-01",
    end: str | None = None,
) -> bt.Cerebro:
    """Build and run a full backtest, returning the Cerebro instance."""
    cerebro = bt.Cerebro()

    # Strategy params from config
    cerebro.addstrategy(
        RSIMeanReversion,
        rsi_period=config.rsi_period,
        rsi_entry=config.rsi_entry,
        rsi_exit=config.rsi_exit,
        sma_period=config.sma_period,
        min_volume=config.min_volume,
        min_price=config.min_price,
        risk_per_trade=config.risk_per_trade,
        stop_loss_pct=config.stop_loss_pct,
        max_positions=config.max_positions,
        max_position_pct=config.max_position_pct,
        time_stop_days=config.time_stop_days,
    )

    # Data feeds
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d") if end else datetime.now()

    feeds_added = 0
    for ticker, df in data.items():
        df = df.loc[
            (df.index >= pd.Timestamp(start_dt)) & (df.index <= pd.Timestamp(end_dt))
        ]
        if len(df) < config.sma_period + 50:
            continue
        feed = bt.feeds.PandasData(dataname=df, name=ticker)
        cerebro.adddata(feed)
        feeds_added += 1

    if feeds_added == 0:
        logger.error("No data feeds added — cannot run backtest")
        return cerebro

    logger.info("Added %d data feeds to backtest", feeds_added)

    # Broker settings
    cerebro.broker.setcash(config.initial_capital)
    cerebro.broker.addcommissioninfo(FixedPerTradeCommission(commission=1.0))
    cerebro.broker.set_slippage_perc(0.001)

    # Analyzers
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe", riskfreerate=0.04)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
    cerebro.addanalyzer(bt.analyzers.Returns, _name="returns")

    logger.info(
        "Starting backtest with $%.2f capital (%s bars) ...",
        config.initial_capital, config.bar_interval,
    )
    results = cerebro.run()
    end_value = cerebro.broker.getvalue()
    if math.isnan(end_value):
        end_value = cerebro.broker.getcash()
    _print_summary(results[0], config.initial_capital, end_value)
    return cerebro


def _print_summary(strat: RSIMeanReversion, start_cash: float, end_cash: float) -> None:
    """Print a clean performance summary to the console."""
    dd = strat.analyzers.drawdown.get_analysis()
    sharpe_data = strat.analyzers.sharpe.get_analysis()

    trades = strat.completed_trades
    total_trades = len(trades)
    won = sum(1 for t in trades if t.pnl_pct > 0)
    lost = total_trades - won
    win_rate = (won / total_trades * 100) if total_trades else 0
    avg_hold = sum(t.bars_held for t in trades) / total_trades if total_trades else 0

    # Exit reason breakdown
    rsi_exits = sum(1 for t in trades if t.exit_reason == "rsi_exit")
    stop_exits = sum(1 for t in trades if t.exit_reason == "stop_loss")
    time_exits = sum(1 for t in trades if t.exit_reason == "time_stop")

    # Average win / loss
    avg_win = sum(t.pnl_pct for t in trades if t.pnl_pct > 0) / won if won else 0
    avg_loss = sum(t.pnl_pct for t in trades if t.pnl_pct <= 0) / lost if lost else 0

    max_dd = dd.get("max", {}).get("drawdown", 0)
    sharpe = sharpe_data.get("sharperatio", None)
    if sharpe is not None and not (isinstance(sharpe, float) and math.isnan(sharpe)):
        sharpe_str = f"{sharpe:.3f}"
    else:
        sharpe_str = "N/A"

    total_return = end_cash / start_cash - 1

    print("\n" + "=" * 60)
    print("  BACKTEST RESULTS")
    print("=" * 60)
    print(f"  Start capital     : ${start_cash:>12,.2f}")
    print(f"  End capital       : ${end_cash:>12,.2f}")
    print(f"  Total return      : {total_return * 100:>11.2f}%")
    print(f"  Total trades      : {total_trades:>12d}")
    print(f"  Won / Lost        : {won:>5d} / {lost:<5d}")
    print(f"  Win rate          : {win_rate:>11.1f}%")
    print(f"  Avg win           : {avg_win:>+11.2f}%")
    print(f"  Avg loss          : {avg_loss:>+11.2f}%")
    print(f"  Avg hold (bars)   : {avg_hold:>11.1f}")
    print(f"  Max drawdown      : {max_dd:>11.2f}%")
    print(f"  Sharpe ratio      : {sharpe_str:>12s}")
    print(f"  ---")
    print(f"  RSI exits         : {rsi_exits:>12d}")
    print(f"  Stop-loss exits   : {stop_exits:>12d}")
    print(f"  Time-stop exits   : {time_exits:>12d}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    from smc_trader.config import Config
    from smc_trader.data import fetch_universe
    from smc_trader.logger import setup_logging

    setup_logging()
    # Quick self-test with a small universe
    cfg = Config(universe=["AAPL", "MSFT", "GOOGL", "AMZN", "META"])
    data = fetch_universe(cfg.universe, start="2009-01-01", pause=0)
    run_backtest(data, cfg, start="2010-01-01")
