"""Position sizing, circuit breakers, and settlement tracking.

All risk-management logic is centralised here so that both the
backtest engine and the live scheduler can share the same rules.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Literal, Tuple

logger = logging.getLogger(__name__)


def calculate_shares(
    equity: float,
    entry_price: float,
    stop_pct: float,
    risk_pct: float,
    max_pos_pct: float,
) -> int:
    """Determine the integer number of shares to buy.

    The position size is the *smaller* of:
    * risk-based:  ``equity * risk_pct / (entry_price * stop_pct)``
    * cap-based:   ``equity * max_pos_pct / entry_price``

    Returns 0 if the entry price is non-positive.
    """
    if entry_price <= 0:
        return 0
    risk_shares = (equity * risk_pct) / (entry_price * stop_pct)
    cap_shares = (equity * max_pos_pct) / entry_price
    shares = int(math.floor(min(risk_shares, cap_shares)))
    return max(shares, 0)


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------

class CircuitBreaker:
    """Tracks equity drawdowns on daily / weekly / monthly horizons.

    Call ``reset_day`` / ``reset_week`` / ``reset_month`` at the start of each
    period, then ``check(current_equity)`` to see whether trading should
    continue.
    """

    DAILY_LIMIT: float = 0.03
    WEEKLY_LIMIT: float = 0.05
    MONTHLY_LIMIT: float = 0.10

    def __init__(self, equity: float) -> None:
        self.day_start: float = equity
        self.week_start: float = equity
        self.month_start: float = equity

    def reset_day(self, equity: float) -> None:
        """Mark the start-of-day equity snapshot."""
        self.day_start = equity

    def reset_week(self, equity: float) -> None:
        """Mark the start-of-week equity snapshot."""
        self.week_start = equity

    def reset_month(self, equity: float) -> None:
        """Mark the start-of-month equity snapshot."""
        self.month_start = equity

    def check(self, current_equity: float) -> Literal["OK", "HALT_DAY", "HALT_WEEK", "KILL"]:
        """Evaluate drawdown thresholds and return the system state.

        * ``OK``        — trading may continue.
        * ``HALT_DAY``  — daily loss limit hit; pause until next session.
        * ``HALT_WEEK`` — weekly loss limit hit; pause until next week.
        * ``KILL``      — monthly loss limit hit; cancel everything.
        """
        day_dd = 1.0 - current_equity / self.day_start if self.day_start else 0.0
        week_dd = 1.0 - current_equity / self.week_start if self.week_start else 0.0
        month_dd = 1.0 - current_equity / self.month_start if self.month_start else 0.0

        if month_dd >= self.MONTHLY_LIMIT:
            logger.critical(
                "KILL — monthly drawdown %.2f%% >= %.2f%%",
                month_dd * 100,
                self.MONTHLY_LIMIT * 100,
            )
            return "KILL"
        if week_dd >= self.WEEKLY_LIMIT:
            logger.warning(
                "HALT_WEEK — weekly drawdown %.2f%% >= %.2f%%",
                week_dd * 100,
                self.WEEKLY_LIMIT * 100,
            )
            return "HALT_WEEK"
        if day_dd >= self.DAILY_LIMIT:
            logger.warning(
                "HALT_DAY — daily drawdown %.2f%% >= %.2f%%",
                day_dd * 100,
                self.DAILY_LIMIT * 100,
            )
            return "HALT_DAY"
        return "OK"


# ---------------------------------------------------------------------------
# Settlement tracker (cash / Reg-T accounts)
# ---------------------------------------------------------------------------

@dataclass
class _PendingSettlement:
    proceeds: float
    settlement_date: date


@dataclass
class SettlementTracker:
    """Track settled vs unsettled cash for a cash account (T+1 equities).

    Call ``record_sale`` when a sale fills and ``record_purchase`` when a buy
    fills.  ``can_enter`` returns whether the account has enough *settled*
    cash for a new purchase.
    """

    settled_cash: float = 0.0
    _pending: List[_PendingSettlement] = field(default_factory=list)

    def _settle(self) -> None:
        """Move any matured pending settlements into settled cash."""
        today = date.today()
        still_pending: List[_PendingSettlement] = []
        for p in self._pending:
            if p.settlement_date <= today:
                self.settled_cash += p.proceeds
                logger.debug("Settled $%.2f", p.proceeds)
            else:
                still_pending.append(p)
        self._pending = still_pending

    def record_sale(self, proceeds: float, settlement_date: date | None = None) -> None:
        """Record a sale.  Proceeds settle on *settlement_date* (default T+1)."""
        if settlement_date is None:
            settlement_date = date.today() + timedelta(days=1)
        self._pending.append(_PendingSettlement(proceeds, settlement_date))
        logger.info(
            "Sale recorded: $%.2f settling on %s", proceeds, settlement_date
        )

    def record_purchase(self, cost: float) -> None:
        """Debit settled cash for a purchase."""
        self._settle()
        self.settled_cash -= cost
        logger.info("Purchase recorded: $%.2f (settled cash now $%.2f)", cost, self.settled_cash)

    def can_enter(self, cost: float) -> bool:
        """Return True if settled cash covers *cost*."""
        self._settle()
        return self.settled_cash >= cost


if __name__ == "__main__":
    from smc_trader.logger import setup_logging

    setup_logging()

    # Position sizing demo
    shares = calculate_shares(
        equity=20_000, entry_price=150.0, stop_pct=0.05,
        risk_pct=0.015, max_pos_pct=0.25,
    )
    print(f"Shares for $150 stock, $20k equity: {shares}")

    # Circuit breaker demo
    cb = CircuitBreaker(equity=20_000)
    print(f"CB at $20,000: {cb.check(20_000)}")
    print(f"CB at $19,500: {cb.check(19_500)}")
    print(f"CB at $19,000: {cb.check(19_000)}")
    print(f"CB at $18,000: {cb.check(18_000)}")

    # Settlement demo
    st = SettlementTracker(settled_cash=10_000)
    print(f"Can enter $5000: {st.can_enter(5000)}")
    st.record_purchase(5000)
    print(f"Settled after buy: ${st.settled_cash:.2f}")
    st.record_sale(5200, settlement_date=date.today())
    st._settle()
    print(f"After same-day settle: ${st.settled_cash:.2f}")
