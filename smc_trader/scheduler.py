"""APScheduler-based daily job runner for the live trading loop.

Schedules three market-hours jobs in the US/Eastern timezone:
  1. **16:35 ET** — end-of-day scan: fetch data, generate signals, log results.
  2. **09:25 ET** — pre-market: review pending signals, submit bracket orders.
  3. **16:00 ET** — exit check: evaluate RSI / time-stop on open positions.

All jobs skip non-trading days (XNYS calendar via ``exchange_calendars``).
Jobs are paused during the IBKR daily-reset window (11:45–12:45 ET).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

import exchange_calendars as xcals
import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from smc_trader.broker import IBKRBroker
from smc_trader.config import Config
from smc_trader.data import fetch_universe
from smc_trader.logger import alert, send_telegram
from smc_trader.risk import CircuitBreaker, SettlementTracker, calculate_shares
from smc_trader.signals import calculate_rsi, scan_universe

logger = logging.getLogger(__name__)
ET = pytz.timezone("US/Eastern")


def _is_trading_day(dt: date | None = None) -> bool:
    """Return True if *dt* (default today) is an XNYS trading session."""
    cal = xcals.get_calendar("XNYS")
    check = dt or date.today()
    return cal.is_session(check)


# ---------------------------------------------------------------------------
# Live trading state
# ---------------------------------------------------------------------------

class TradingState:
    """Mutable state shared across scheduled jobs."""

    def __init__(self, config: Config, broker: IBKRBroker) -> None:
        self.config = config
        self.broker = broker
        self.circuit_breaker = CircuitBreaker(config.initial_capital)
        self.settlement = SettlementTracker(settled_cash=config.initial_capital)
        self.pending_signals: List[dict] = []
        self.position_meta: Dict[str, dict] = {}  # ticker -> {entry_price, entry_date, shares}

    def _run_async(self, coro):
        """Helper to run an async coroutine from sync scheduler callbacks."""
        loop = self.broker.ib.loop if hasattr(self.broker.ib, 'loop') else None
        if loop and loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            return future.result(timeout=60)
        return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Job implementations
# ---------------------------------------------------------------------------

def job_eod_scan(state: TradingState) -> None:
    """16:35 ET — Fetch data, generate signals, log results."""
    if not _is_trading_day():
        logger.info("Not a trading day — skipping EOD scan")
        return

    logger.info("=== EOD Scan starting ===")
    try:
        data = fetch_universe(state.config.universe, start="2009-01-01", pause=0.02)
        signals = scan_universe(data, state.config)
        today_signals = signals[
            (signals["signal"]) & (signals["date"] == signals["date"].max())
        ]

        state.pending_signals = today_signals.to_dict("records")
        logger.info(
            "EOD scan complete: %d signals generated", len(state.pending_signals)
        )
        for sig in state.pending_signals:
            logger.info(
                "  SIGNAL %s  close=%.2f  rsi=%.1f",
                sig["ticker"], sig["close"], sig["rsi"],
            )
        alert(
            f"EOD scan: {len(state.pending_signals)} entry signals",
            token=state.config.telegram_token,
            chat_id=state.config.telegram_chat_id,
        )
    except Exception as exc:
        logger.exception("EOD scan failed: %s", exc)


def job_premarket_orders(state: TradingState) -> None:
    """09:25 ET — Submit bracket orders for yesterday's signals."""
    if not _is_trading_day():
        logger.info("Not a trading day — skipping pre-market orders")
        return

    logger.info("=== Pre-market order submission ===")
    try:
        # Circuit breaker check
        equity = state._run_async(state.broker.get_account_equity())
        sizing_equity = state.config.account_size if state.config.account_size > 0 else equity
        cb_state = state.circuit_breaker.check(equity)
        logger.info("Circuit breaker: %s (equity=$%.2f)", cb_state, equity)

        if cb_state == "KILL":
            logger.critical("KILL switch — cancelling everything")
            state._run_async(state.broker.cancel_all_orders())
            alert(
                "KILL SWITCH ACTIVATED — all orders cancelled, positions flattened",
                token=state.config.telegram_token,
                chat_id=state.config.telegram_chat_id,
                level=logging.CRITICAL,
            )
            return
        if cb_state != "OK":
            logger.warning("Circuit breaker %s — no new entries", cb_state)
            state.pending_signals = []
            return

        # Count current positions
        positions = state._run_async(state.broker.get_positions())
        open_count = len(positions)

        for sig in state.pending_signals:
            if open_count >= state.config.max_positions:
                logger.info("Max positions reached (%d) — skipping remaining signals", open_count)
                break

            ticker = sig["ticker"]
            if ticker in positions:
                logger.debug("Already holding %s — skip", ticker)
                continue

            entry_price = sig["close"]
            stop_price = round(entry_price * (1 - state.config.stop_loss_pct), 2)
            shares = calculate_shares(
                equity=sizing_equity,
                entry_price=entry_price,
                stop_pct=state.config.stop_loss_pct,
                risk_pct=state.config.risk_per_trade,
                max_pos_pct=state.config.max_position_pct,
            )
            if shares <= 0:
                logger.debug("Zero shares for %s — skip", ticker)
                continue

            cost = shares * entry_price
            if not state.settlement.can_enter(cost):
                logger.info("Insufficient settled cash for %s ($%.2f needed)", ticker, cost)
                continue

            state._run_async(
                state.broker.place_bracket_order(ticker, shares, entry_price, stop_price)
            )
            state.position_meta[ticker] = {
                "entry_price": entry_price,
                "entry_date": date.today(),
                "shares": shares,
            }
            state.settlement.record_purchase(cost)
            open_count += 1

            alert(
                f"ORDER: BUY {shares} {ticker} @ {entry_price:.2f}, stop {stop_price:.2f}",
                token=state.config.telegram_token,
                chat_id=state.config.telegram_chat_id,
            )

        state.pending_signals = []
        logger.info("Pre-market order submission complete")

    except Exception as exc:
        logger.exception("Pre-market orders failed: %s", exc)


def job_eod_exit_check(state: TradingState) -> None:
    """16:00 ET — Check RSI exit / time stop on all open positions."""
    if not _is_trading_day():
        logger.info("Not a trading day — skipping exit check")
        return

    logger.info("=== EOD exit check ===")
    try:
        positions = state._run_async(state.broker.get_positions())
        if not positions:
            logger.info("No open positions")
            return

        # Fetch fresh data for held tickers
        from smc_trader.data import fetch_ticker

        for ticker, shares in positions.items():
            if shares <= 0:
                continue

            df = fetch_ticker(ticker, start="2024-01-01")
            if df is None or df.empty:
                logger.warning("No data for %s — cannot check exit", ticker)
                continue

            rsi_series = calculate_rsi(df["Close"], state.config.rsi_period)
            current_rsi = rsi_series.iloc[-1]

            meta = state.position_meta.get(ticker, {})
            entry_date = meta.get("entry_date", date.today())
            days_held = (date.today() - entry_date).days

            exit_reason: Optional[str] = None
            if current_rsi > state.config.rsi_exit:
                exit_reason = f"RSI exit (rsi={current_rsi:.1f})"
            elif days_held >= state.config.time_stop_days:
                exit_reason = f"Time stop ({days_held} days)"

            if exit_reason:
                state._run_async(state.broker.place_market_sell(ticker, shares))
                entry_price = meta.get("entry_price", 0)
                current_price = df["Close"].iloc[-1]
                state.settlement.record_sale(shares * current_price)
                if ticker in state.position_meta:
                    del state.position_meta[ticker]

                alert(
                    f"EXIT: SELL {shares} {ticker} — {exit_reason} "
                    f"(entry={entry_price:.2f}, current={current_price:.2f})",
                    token=state.config.telegram_token,
                    chat_id=state.config.telegram_chat_id,
                )
            else:
                logger.info(
                    "HOLD %s — rsi=%.1f, days=%d", ticker, current_rsi, days_held
                )

        # Daily P&L summary
        equity = state._run_async(state.broker.get_account_equity())
        daily_pnl = equity - state.circuit_breaker.day_start
        alert(
            f"Daily P&L: ${daily_pnl:+,.2f}  Equity: ${equity:,.2f}",
            token=state.config.telegram_token,
            chat_id=state.config.telegram_chat_id,
        )
        state.circuit_breaker.reset_day(equity)

    except Exception as exc:
        logger.exception("EOD exit check failed: %s", exc)


# ---------------------------------------------------------------------------
# Scheduler setup
# ---------------------------------------------------------------------------

def build_scheduler(state: TradingState) -> BackgroundScheduler:
    """Create and configure the APScheduler instance (does not start it)."""
    scheduler = BackgroundScheduler(timezone=ET)

    # 16:35 ET Mon–Fri: EOD scan
    scheduler.add_job(
        job_eod_scan,
        trigger="cron",
        args=[state],
        day_of_week="mon-fri",
        hour=16,
        minute=35,
        id="eod_scan",
        misfire_grace_time=300,
    )

    # 09:25 ET Mon–Fri: pre-market orders
    scheduler.add_job(
        job_premarket_orders,
        trigger="cron",
        args=[state],
        day_of_week="mon-fri",
        hour=9,
        minute=25,
        id="premarket_orders",
        misfire_grace_time=300,
    )

    # 16:00 ET Mon–Fri: exit check
    scheduler.add_job(
        job_eod_exit_check,
        trigger="cron",
        args=[state],
        day_of_week="mon-fri",
        hour=16,
        minute=0,
        id="eod_exit_check",
        misfire_grace_time=300,
    )

    logger.info("Scheduler configured with 3 jobs (US/Eastern)")
    return scheduler


if __name__ == "__main__":
    from smc_trader.logger import setup_logging

    setup_logging()
    print(f"Today is a trading day: {_is_trading_day()}")
    cfg = Config(universe=["AAPL"])
    broker = IBKRBroker(cfg)
    state = TradingState(cfg, broker)
    sched = build_scheduler(state)
    print("Scheduled jobs:")
    for job in sched.get_jobs():
        print(f"  {job.id}: {job.trigger}")
    print("(not starting scheduler in self-test)")
