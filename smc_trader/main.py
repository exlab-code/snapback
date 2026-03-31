"""Entry point for the SMC swing trading system.

Usage::

    python -m smc_trader.main backtest   # run historical backtest
    python -m smc_trader.main live       # connect to IBKR and start scheduler
    python -m smc_trader.main kill       # cancel all orders and flatten positions
"""

from __future__ import annotations

import asyncio
import signal
import sys
import time

from smc_trader.config import Config
from smc_trader.logger import setup_logging


def cmd_backtest() -> None:
    """Run the Backtrader backtest and print results."""
    import logging

    from smc_trader.backtest import run_backtest
    from smc_trader.data import fetch_universe

    logger = logging.getLogger(__name__)
    cfg = Config()
    logger.info(
        "Backtest mode — fetching %s data for %d tickers ...",
        cfg.bar_interval, len(cfg.universe),
    )
    data = fetch_universe(cfg.universe, start="2009-01-01", interval=cfg.bar_interval)
    run_backtest(data, cfg, start="2010-01-01")


def cmd_live() -> None:
    """Connect to IBKR paper and start the scheduler loop."""
    import logging

    from smc_trader.scheduler import TradingState, build_scheduler

    logger = logging.getLogger(__name__)
    cfg = Config()

    state = TradingState(cfg)
    scheduler = build_scheduler(state)
    scheduler.start()

    logger.info("Scheduler running — press Ctrl+C to stop")
    print("\nScheduler running.  Press Ctrl+C to shut down.\n")
    print("Scheduled jobs:")
    for job in scheduler.get_jobs():
        print(f"  {job.id}: next run at {job.next_run_time}")
    print()

    # Graceful shutdown
    shutdown = False

    def _handle_signal(signum, frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:
        while not shutdown:
            time.sleep(1)
    finally:
        logger.info("Shutting down ...")
        scheduler.shutdown(wait=False)
        logger.info("Shutdown complete")


def cmd_kill() -> None:
    """Connect to IBKR, cancel all orders, flatten positions, disconnect."""
    import logging

    from smc_trader.broker import IBKRBroker

    logger = logging.getLogger(__name__)
    cfg = Config()
    broker = IBKRBroker(cfg)

    async def _kill():
        await broker.connect()
        logger.warning("KILL SWITCH — cancelling all orders and flattening positions")
        await broker.cancel_all_orders()
        await broker.disconnect()

    logger.info("Kill mode — connecting to IBKR ...")
    asyncio.run(_kill())
    logger.info("Kill complete")


COMMANDS = {
    "backtest": cmd_backtest,
    "live": cmd_live,
    "kill": cmd_kill,
}


def main() -> None:
    """Parse CLI args and dispatch to the appropriate command."""
    setup_logging()

    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(f"Usage: python -m smc_trader.main <{'|'.join(COMMANDS)}>")
        sys.exit(1)

    COMMANDS[sys.argv[1]]()


if __name__ == "__main__":
    main()
