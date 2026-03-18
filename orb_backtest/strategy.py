"""
ORB backtest using Backtrader on 1-minute Massive.com data.

Run:
    python3 orb_backtest/strategy.py

Requires minute_cache/ directory populated by fetch_data.py.
"""
from __future__ import annotations

import math
import os
import warnings
warnings.filterwarnings("ignore")

import backtrader as bt
import pandas as pd
import numpy as np
from datetime import time, date, timedelta
from dataclasses import dataclass, field


# ── parameters ───────────────────────────────────────────────────────────────
OR_MINUTES       = 15       # 9:30–9:44 inclusive
MIN_GAP_PCT      = 0.005    # 0.5% gap to qualify (open vs prev close)
VOL_CONFIRM_MULT = 1.3      # breakout bar volume >= mult * rolling avg
RISK_PER_TRADE   = 0.015    # 1.5% equity risk
MAX_POS_PCT      = 0.40     # max 40% of equity per position
MAX_POSITIONS    = 3
TRADE_END        = time(11, 0)   # no new entries after 11:00
EOD_EXIT         = time(15, 45)  # flatten at 15:45
INITIAL_CAPITAL  = 20_000.0
COMMISSION       = 0.005    # $0.005/share
MIN_COMMISSION   = 1.0

CACHE_DIR = os.path.join(os.path.dirname(__file__), "minute_cache")


# ── commission ────────────────────────────────────────────────────────────────
class PerShareCommission(bt.CommInfoBase):
    params = (
        ("commission", COMMISSION),
        ("min_comm", MIN_COMMISSION),
        ("stocklike", True),
        ("commtype", bt.CommInfoBase.COMM_FIXED),
    )

    def _getcommission(self, size, price, pseudoexec):
        comm = abs(size) * self.p.commission
        return max(comm, self.p.min_comm) if not pseudoexec else comm


# ── per-stock state ────────────────────────────────────────────────────────────
@dataclass
class StockState:
    or_high: float = 0.0
    or_low: float = float("inf")
    or_complete: bool = False
    bars_in_or: int = 0
    traded_today: bool = False
    prev_close: float = 0.0
    day_open: float = 0.0
    qualified: bool = False          # passed gap filter today
    recent_vols: list = field(default_factory=list)
    vol_avg: float = 0.0
    current_date: date = field(default_factory=date.today)
    pending_entry: dict = field(default_factory=dict)


# ── strategy ──────────────────────────────────────────────────────────────────
class ORBStrategy(bt.Strategy):

    def __init__(self):
        self.states: dict[int, StockState] = {}
        self.stop_orders: dict[int, bt.Order] = {}
        self.target_orders: dict[int, bt.Order] = {}
        self.trades: list[dict] = []

        for i in range(len(self.datas)):
            self.states[i] = StockState()
            self.stop_orders[i] = None
            self.target_orders[i] = None

    # helpers
    def _equity(self) -> float:
        v = self.broker.getvalue()
        return v if not math.isnan(v) else self.broker.getcash()

    def _open_position_count(self) -> int:
        return sum(1 for d in self.datas if self.getposition(d).size > 0)

    def _reset_day(self, idx: int, d):
        s = self.states[idx]
        s.or_high = 0.0
        s.or_low = float("inf")
        s.or_complete = False
        s.bars_in_or = 0
        s.traded_today = False
        s.qualified = False
        s.recent_vols = []
        s.vol_avg = 0.0
        s.pending_entry = {}
        # Store today's open and yesterday's close for gap calc
        s.day_open = d.open[0]
        # prev_close was set at EOD yesterday

    def next(self):
        for idx, d in enumerate(self.datas):
            self._process(idx, d)

    def _process(self, idx: int, d):
        s = self.states[idx]
        pos = self.getposition(d)

        try:
            bar_dt = bt.num2date(d.datetime[0])
            bar_time = bar_dt.time()
            bar_date = bar_dt.date()
        except Exception:
            return

        # ── new day reset ──
        if bar_date != s.current_date:
            # save previous day close
            if len(d) > 1:
                s.prev_close = d.close[-1]
            s.current_date = bar_date
            self._reset_day(idx, d)
            # gap filter
            if s.prev_close > 0 and s.day_open > 0:
                gap = (s.day_open - s.prev_close) / s.prev_close
                s.qualified = gap >= MIN_GAP_PCT
            return

        # ── EOD flatten ──
        if bar_time >= EOD_EXIT:
            if pos.size > 0:
                self._flatten(idx, d)
            return

        if bar_time < time(9, 30):
            return

        # ── OR accumulation (9:30 to 9:30+OR_MINUTES-1) ──
        if not s.or_complete:
            bar_minute = bar_time.hour * 60 + bar_time.minute
            or_start = 9 * 60 + 30
            if or_start <= bar_minute < or_start + OR_MINUTES:
                s.or_high = max(s.or_high, d.high[0])
                s.or_low = min(s.or_low, d.low[0])
                s.bars_in_or += 1
                s.recent_vols.append(d.volume[0])
                if s.bars_in_or >= OR_MINUTES:
                    s.or_complete = True
                    s.vol_avg = (
                        sum(s.recent_vols) / len(s.recent_vols)
                        if s.recent_vols else 0.0
                    )
            return

        # ── maintain rolling volume avg ──
        s.recent_vols.append(d.volume[0])
        if len(s.recent_vols) > 20:
            s.recent_vols.pop(0)
        if s.recent_vols:
            s.vol_avg = sum(s.recent_vols) / len(s.recent_vols)

        # ── manage open position ──
        if pos.size > 0:
            # stop and target managed as separate orders — nothing to do here
            return

        # ── entry conditions ──
        if s.traded_today or not s.qualified:
            return
        if bar_time > TRADE_END:
            return
        if self._open_position_count() >= MAX_POSITIONS:
            return
        if s.or_high <= 0 or s.or_low >= s.or_high:
            return

        # Long breakout: close above OR high, close in top 30% of bar, volume confirm
        bar_range = d.high[0] - d.low[0]
        close_in_top = (
            bar_range > 0
            and (d.close[0] - d.low[0]) / bar_range >= 0.70
        )
        vol_ok = (
            s.vol_avg <= 0
            or d.volume[0] >= s.vol_avg * VOL_CONFIRM_MULT
        )

        if d.close[0] > s.or_high and close_in_top and vol_ok:
            equity = self._equity()
            entry = d.close[0]
            stop = s.or_low
            if entry <= stop:
                return
            stop_dist = entry - stop

            risk_shares = int((equity * RISK_PER_TRADE) / stop_dist)
            cap_shares  = int((equity * MAX_POS_PCT) / entry)
            shares = max(min(risk_shares, cap_shares), 0)
            if shares <= 0:
                return

            target = round(entry + (s.or_high - s.or_low), 2)

            self.buy(data=d, size=shares, exectype=bt.Order.Market)
            s.traded_today = True

            # Cache entry details; stop/target orders placed in notify_order
            s.pending_entry = {
                "stop":       round(stop, 2),
                "target":     target,
                "shares":     shares,
                "entry":      entry,
                "entry_time": bar_dt,
                "or_high":    s.or_high,
                "or_low":     s.or_low,
            }

    def _flatten(self, idx: int, d):
        if self.getposition(d).size > 0:
            self.close(data=d)
        # Cancel any pending stop / target orders
        for o in [self.stop_orders.get(idx), self.target_orders.get(idx)]:
            if o is not None and o.status in (
                bt.Order.Submitted, bt.Order.Accepted, bt.Order.Partial
            ):
                self.cancel(o)
        self.stop_orders[idx] = None
        self.target_orders[idx] = None

    def notify_order(self, order):
        if order.status not in (order.Completed,):
            return

        d = order.data
        idx = self.datas.index(d)
        s = self.states[idx]

        if order.isbuy() and order.status == order.Completed:
            pending = s.pending_entry
            if not pending:
                return
            # Place OCO-style stop + limit target
            stop_ord = self.sell(
                data=d,
                size=order.executed.size,
                exectype=bt.Order.Stop,
                price=pending["stop"],
            )
            target_ord = self.sell(
                data=d,
                size=order.executed.size,
                exectype=bt.Order.Limit,
                price=pending["target"],
            )
            self.stop_orders[idx] = stop_ord
            self.target_orders[idx] = target_ord

        elif order.issell() and order.status == order.Completed:
            pending = s.pending_entry
            if pending:
                pnl = (
                    (order.executed.price - pending["entry"])
                    * order.executed.size
                )
                self.trades.append({
                    "symbol":     d._name,
                    "entry":      pending["entry"],
                    "exit":       order.executed.price,
                    "shares":     order.executed.size,
                    "pnl":        pnl,
                    "pnl_pct":    (order.executed.price / pending["entry"] - 1) * 100,
                    "or_high":    pending["or_high"],
                    "or_low":     pending["or_low"],
                    "entry_time": pending.get("entry_time"),
                })
                s.pending_entry = {}

            # Cancel sibling order (OCO simulation)
            stop_ord   = self.stop_orders.get(idx)
            target_ord = self.target_orders.get(idx)
            if stop_ord is not None and stop_ord.ref == order.ref:
                other = target_ord
            elif target_ord is not None and target_ord.ref == order.ref:
                other = stop_ord
            else:
                other = None

            if other is not None and other.status in (
                bt.Order.Submitted, bt.Order.Accepted, bt.Order.Partial
            ):
                self.cancel(other)

            self.stop_orders[idx] = None
            self.target_orders[idx] = None


# ── load data ─────────────────────────────────────────────────────────────────
def load_feeds(start: str = "2023-01-01") -> list[tuple[str, bt.feeds.PandasData]]:
    feeds = []
    if not os.path.isdir(CACHE_DIR):
        raise RuntimeError(
            f"No minute cache at {CACHE_DIR}. Run fetch_data.py first."
        )

    files = sorted(f for f in os.listdir(CACHE_DIR) if f.endswith("_1min.parquet"))
    if not files:
        raise RuntimeError(
            "No parquet files found in minute_cache/. Run fetch_data.py first."
        )

    for fname in files:
        ticker = fname.replace("_1min.parquet", "")
        df = pd.read_parquet(os.path.join(CACHE_DIR, fname))
        df = df[df.index >= start].copy()
        if df.empty:
            continue
        df.index = pd.to_datetime(df.index)
        feed = bt.feeds.PandasData(
            dataname=df,
            datetime=None,
            open="open",
            high="high",
            low="low",
            close="close",
            volume="volume",
            openinterest=-1,
            timeframe=bt.TimeFrame.Minutes,
            compression=1,
        )
        feeds.append((ticker, feed))

    print(f"Loaded {len(feeds)} tickers from minute cache.")
    return feeds


# ── run ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    feeds = load_feeds(start="2023-01-01")
    if not feeds:
        print("No data. Run: python3 orb_backtest/fetch_data.py")
        raise SystemExit(1)

    cerebro = bt.Cerebro(stdstats=False)
    cerebro.broker.setcash(INITIAL_CAPITAL)
    cerebro.broker.addcommissioninfo(PerShareCommission())
    cerebro.broker.set_slippage_perc(0.001)  # 0.1% slippage on volatile opens

    for ticker, feed in feeds:
        cerebro.adddata(feed, name=ticker)

    cerebro.addstrategy(ORBStrategy)

    print(f"Running ORB backtest on {len(feeds)} stocks (2023–present) ...")
    results = cerebro.run(runonce=False, preload=True)
    strat = results[0]

    # ── summary ──────────────────────────────────────────────────────────────
    end_val   = cerebro.broker.getvalue()
    total_ret = (end_val / INITIAL_CAPITAL - 1) * 100
    trades    = strat.trades

    wins  = [t for t in trades if t["pnl"] > 0]
    loses = [t for t in trades if t["pnl"] <= 0]
    avg_win  = sum(t["pnl"] for t in wins)  / len(wins)  if wins  else 0.0
    avg_loss = sum(t["pnl"] for t in loses) / len(loses) if loses else 0.0

    print()
    print("=" * 50)
    print("  ORB BACKTEST RESULTS")
    print("=" * 50)
    print(f"  Start capital  : ${INITIAL_CAPITAL:>12,.2f}")
    print(f"  End capital    : ${end_val:>12,.2f}")
    print(f"  Total return   : {total_ret:>11.2f}%")
    print(f"  Total trades   : {len(trades):>12}")
    if trades:
        print(f"  Win rate       : {len(wins)/len(trades)*100:>11.1f}%")
    else:
        print("  Win rate       :          n/a")
    print(f"  Avg win        : ${avg_win:>11.2f}")
    print(f"  Avg loss       : ${avg_loss:>11.2f}")
    if loses and avg_loss != 0:
        gross_win  = sum(t["pnl"] for t in wins)
        gross_loss = sum(t["pnl"] for t in loses)
        if gross_loss != 0:
            print(f"  Profit factor  : {abs(gross_win / gross_loss):>12.2f}")
        else:
            print("  Profit factor  :          inf")
    print("=" * 50)

    # top symbols by total P&L
    if trades:
        by_sym: dict[str, list[float]] = {}
        for t in trades:
            by_sym.setdefault(t["symbol"], []).append(t["pnl"])
        print("\nTop symbols by total P&L:")
        for sym, pnls in sorted(by_sym.items(), key=lambda x: -sum(x[1]))[:10]:
            print(f"  {sym:<6} {len(pnls):3d} trades  ${sum(pnls):>8.2f}")

    # ── pyfolio tearsheet ────────────────────────────────────────────────────
    print("\nGenerating tearsheet → orb_backtest/tearsheet.png ...")
    try:
        import pyfolio as pf

        if trades:
            trade_df = pd.DataFrame(trades)
            trade_df["date"] = pd.to_datetime(trade_df["entry_time"]).dt.date
            daily_pnl = trade_df.groupby("date")["pnl"].sum()
            daily_pnl.index = pd.to_datetime(daily_pnl.index)

            # Convert cumulative P&L to a returns series
            equity_curve = daily_pnl.cumsum() + INITIAL_CAPITAL
            returns = equity_curve.pct_change().dropna()
            returns.index = returns.index.tz_localize("UTC")

            fig = pf.create_returns_tear_sheet(returns, return_fig=True)
            out = os.path.join(os.path.dirname(__file__), "tearsheet.png")
            fig.savefig(out, bbox_inches="tight", dpi=150)
            print(f"Saved: {out}")
        else:
            print("No trades — skipping tearsheet.")
    except Exception as e:
        print(f"Tearsheet failed: {e}")
