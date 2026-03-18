"""Entry point for the ORB trading system.

Usage:
    python -m orb_trader.main live
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

import ib_async.util as util

from smc_trader.logger import setup_logging
from orb_trader.config import ORBConfig
from orb_trader.trader import ORBTrader
from orb_trader.scheduler import build_scheduler


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)

    if len(sys.argv) < 2 or sys.argv[1] != "live":
        print("Usage: python -m orb_trader.main live")
        sys.exit(1)

    # Patch asyncio for ib_async compatibility
    util.patchAsyncio()

    cfg = ORBConfig()
    trader = ORBTrader(cfg)

    async def run() -> None:
        logger.info(
            "ORB live mode — connecting to IBKR at %s:%d ...",
            cfg.ibkr_host,
            cfg.ibkr_port,
        )
        await trader.connect()

        scheduler = build_scheduler(trader)
        scheduler.start()

        logger.info("ORB scheduler running — Ctrl+C to stop")

        stop_event = asyncio.Event()

        def _handle_signal(*_) -> None:
            stop_event.set()

        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, _handle_signal)
        loop.add_signal_handler(signal.SIGTERM, _handle_signal)

        await stop_event.wait()

        logger.info("Shutting down ORB trader ...")
        scheduler.shutdown(wait=False)
        await trader.disconnect()

    asyncio.run(run())


if __name__ == "__main__":
    main()
