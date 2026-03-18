"""APScheduler configuration for the ORB trading system.

Uses ``AsyncIOScheduler`` to integrate cleanly with ib_async's event loop.
"""

from __future__ import annotations

import logging
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from orb_trader.config import ORBConfig
from orb_trader.trader import ORBTrader

logger = logging.getLogger(__name__)
ET = ZoneInfo("US/Eastern")


def build_scheduler(trader: ORBTrader) -> AsyncIOScheduler:
    """Construct and return a configured ``AsyncIOScheduler``.

    Jobs registered:
    * **09:15 ET** (Mon–Fri) — pre-market scan and bar subscription.
    * **15:45 ET** (Mon–Fri) — EOD flatten of all open ORB positions.
    """
    scheduler = AsyncIOScheduler(timezone=ET)

    # 9:15 ET: pre-market scan and bar subscription
    scheduler.add_job(
        trader.run_premarket_scan,
        trigger="cron",
        day_of_week="mon-fri",
        hour=9,
        minute=15,
        id="orb_scan",
        misfire_grace_time=300,
    )

    # 15:45 ET: flatten all positions before close
    scheduler.add_job(
        trader.eod_flatten,
        trigger="cron",
        day_of_week="mon-fri",
        hour=15,
        minute=45,
        id="orb_eod_flatten",
        misfire_grace_time=60,
    )

    logger.info(
        "ORB scheduler configured (scan 09:15, flatten 15:45 US/Eastern)"
    )
    return scheduler
