"""Opening Range Breakout signal state machine.

Each tracked symbol gets its own ``ORBState`` instance.  ``process_bar``
advances the state and returns a signal string (``'LONG'``) or ``None``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import time
from zoneinfo import ZoneInfo

from orb_trader.config import ORBConfig

ET = ZoneInfo("US/Eastern")
logger = logging.getLogger(__name__)

_MAX_RECENT_VOLUMES = 20


@dataclass
class ORBState:
    symbol: str
    or_high: float = 0.0
    or_low: float = float("inf")
    or_complete: bool = False
    bars_collected: int = 0
    traded_today: bool = False
    recent_volumes: list = field(default_factory=list)
    vol_avg: float = 0.0
    entry_price: float = 0.0
    stop_price: float = 0.0
    or_width: float = 0.0


def process_bar(state: ORBState, bar, config: ORBConfig) -> str | None:
    """Process a completed 1-min bar and advance the ORB state machine.

    Parameters
    ----------
    state:
        Mutable state object for this symbol.
    bar:
        A completed bar object with ``.date``, ``.open``, ``.high``,
        ``.low``, ``.close``, ``.volume`` attributes.
    config:
        ORBConfig with strategy parameters.

    Returns
    -------
    ``'LONG'`` when a long breakout signal fires, otherwise ``None``.
    """
    # Resolve bar time to Eastern
    bar_dt = bar.date
    if hasattr(bar_dt, "astimezone"):
        bar_time = bar_dt.astimezone(ET).time()
    else:
        bar_time = bar_dt  # assume already a time object

    market_open = time(9, 30)
    or_end = time(9, 30 + config.or_minutes) if config.or_minutes < 30 else _add_minutes(time(9, 30), config.or_minutes)
    trade_end = time(config.trade_end_hour, config.trade_end_minute)

    # --- Update rolling volume average ---
    state.recent_volumes.append(bar.volume)
    if len(state.recent_volumes) > _MAX_RECENT_VOLUMES:
        state.recent_volumes.pop(0)
    if state.recent_volumes:
        state.vol_avg = sum(state.recent_volumes) / len(state.recent_volumes)

    sym = state.symbol

    # --- Phase 1: accumulate OR high/low (9:30 to 9:30+or_minutes) ---
    if market_open <= bar_time < or_end:
        if bar.high > state.or_high:
            state.or_high = bar.high
        if bar.low < state.or_low:
            state.or_low = bar.low
        state.bars_collected += 1
        logger.debug(
            "%s OR bar %d  H=%.2f L=%.2f  or_high=%.2f or_low=%.2f",
            sym, state.bars_collected, bar.high, bar.low, state.or_high, state.or_low,
        )
        return None

    # --- Mark OR complete on the first bar after the OR window ---
    if not state.or_complete and bar_time >= or_end:
        state.or_complete = True
        if state.or_low == float("inf"):
            state.or_low = 0.0
        state.or_width = state.or_high - state.or_low
        logger.info(
            "%s OR complete  high=%.2f  low=%.2f  width=%.2f (%.1f%%)",
            sym, state.or_high, state.or_low, state.or_width,
            state.or_width / state.or_low * 100 if state.or_low else 0,
        )

    # --- Phase 2: detect breakout (OR complete, until trade_end) ---
    if not state.or_complete:
        return None

    if bar_time >= trade_end:
        return None

    if state.traded_today:
        return None

    # Long breakout conditions
    # 1. Close above OR high
    if bar.close <= state.or_high:
        logger.debug(
            "%s  %s  close=%.2f  below or_high=%.2f  no breakout",
            sym, bar_time.strftime("%H:%M"), bar.close, state.or_high,
        )
        return None

    # 2. Volume confirmation (if we have a vol average)
    if state.vol_avg > 0 and bar.volume < state.vol_avg * config.vol_confirm_mult:
        logger.info(
            "%s  %s  close=%.2f > or_high=%.2f  SKIP: vol %d < %.0f×avg (need %.0f)",
            sym, bar_time.strftime("%H:%M"), bar.close, state.or_high,
            bar.volume, config.vol_confirm_mult, state.vol_avg * config.vol_confirm_mult,
        )
        return None

    # 3. Close in top 30% of bar range (bullish engulfing / momentum)
    bar_range = bar.high - bar.low
    if bar_range > 0:
        close_position = (bar.close - bar.low) / bar_range
        if close_position < 0.70:
            logger.info(
                "%s  %s  close=%.2f > or_high=%.2f  SKIP: close at %.0f%% of range (need 70%%)",
                sym, bar_time.strftime("%H:%M"), bar.close, state.or_high, close_position * 100,
            )
            return None

    logger.info(
        "%s  %s  SIGNAL LONG  close=%.2f  or_high=%.2f  vol=%d (avg=%.0f)  close_pos=%.0f%%",
        sym, bar_time.strftime("%H:%M"), bar.close, state.or_high,
        bar.volume, state.vol_avg,
        ((bar.close - bar.low) / bar_range * 100) if bar_range > 0 else 100,
    )
    return "LONG"


def _add_minutes(t: time, minutes: int) -> time:
    """Add *minutes* to a ``datetime.time`` object (no date overflow)."""
    total = t.hour * 60 + t.minute + minutes
    return time(total // 60, total % 60)
