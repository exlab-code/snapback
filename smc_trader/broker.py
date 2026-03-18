"""IBKR broker integration via ib_async.

Provides connection management, account queries, order placement, and a
kill-switch to flatten all positions.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

from ib_async import IB, Contract, LimitOrder, MarketOrder, Order, StopOrder, Trade

from smc_trader.config import Config

logger = logging.getLogger(__name__)


class IBKRBroker:
    """Wrapper around ib_async for paper / live trading with IBKR.

    Usage::

        broker = IBKRBroker(config)
        await broker.connect()
        equity = await broker.get_account_equity()
        ...
        await broker.disconnect()
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self.ib = IB()
        self._connected = False

    # -- connection ----------------------------------------------------------

    async def connect(self, max_retries: int = 10, backoff: float = 30.0) -> None:
        """Connect to TWS / IB Gateway with retry logic.

        Retries up to *max_retries* times, sleeping *backoff* seconds between
        attempts.  Requests delayed market data (type 4) suitable for paper
        trading without a live data subscription.
        """
        for attempt in range(1, max_retries + 1):
            try:
                await self.ib.connectAsync(
                    host=self.config.ibkr_host,
                    port=self.config.ibkr_port,
                    clientId=self.config.ibkr_client_id,
                    readonly=False,
                )
                self.ib.reqMarketDataType(4)  # delayed / frozen data
                self._connected = True
                logger.info(
                    "Connected to IBKR on %s:%d (attempt %d)",
                    self.config.ibkr_host,
                    self.config.ibkr_port,
                    attempt,
                )
                return
            except Exception as exc:
                logger.warning(
                    "Connection attempt %d/%d failed: %s",
                    attempt, max_retries, exc,
                )
                if attempt < max_retries:
                    await asyncio.sleep(backoff)
        raise ConnectionError(
            f"Could not connect to IBKR after {max_retries} attempts"
        )

    async def disconnect(self) -> None:
        """Disconnect from IBKR."""
        if self._connected:
            self.ib.disconnect()
            self._connected = False
            logger.info("Disconnected from IBKR")

    # -- account queries -----------------------------------------------------

    async def get_account_equity(self) -> float:
        """Return the current NetLiquidation value.

        Prefers USD; falls back to BASE (EUR) for non-USD accounts.
        """
        summary = await self.ib.accountSummaryAsync()
        base_val = 0.0
        for item in summary:
            if item.tag == "NetLiquidation":
                if item.currency == "USD":
                    return float(item.value)
                if item.currency in ("EUR", "BASE"):
                    base_val = float(item.value)
        if base_val:
            logger.info("NetLiquidation (base currency): %.2f", base_val)
            return base_val
        logger.error("Could not retrieve NetLiquidation")
        return 0.0

    async def get_positions(self) -> Dict[str, int]:
        """Return ``{ticker: signed_shares}`` for all open positions."""
        positions = self.ib.positions()
        result: Dict[str, int] = {}
        for pos in positions:
            ticker = pos.contract.symbol
            shares = int(pos.position)
            if shares != 0:
                result[ticker] = shares
        return result

    async def get_open_orders(self) -> List[Trade]:
        """Return a list of pending / submitted trades."""
        return self.ib.openTrades()

    # -- order placement -----------------------------------------------------

    def _stock_contract(self, ticker: str) -> Contract:
        """Build a US equity contract."""
        contract = Contract()
        contract.symbol = ticker
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        return contract

    async def place_bracket_order(
        self,
        ticker: str,
        shares: int,
        entry_price: float,
        stop_price: float,
    ) -> Tuple[Trade, Trade]:
        """Place a limit-entry order with a linked stop-loss order.

        The entry is a GTC LMT order at *entry_price*.
        The stop is a GTC STP order at *stop_price*.
        The RSI-based profit exit is managed in software (the scheduler checks
        RSI at end-of-day and submits a MKT order when triggered).

        Returns ``(entry_trade, stop_trade)``.
        """
        contract = self._stock_contract(ticker)
        await self.ib.qualifyContractsAsync(contract)

        # Entry order — LMT GTC
        entry_order = LimitOrder(
            action="BUY",
            totalQuantity=shares,
            lmtPrice=round(entry_price, 2),
            tif="GTC",
            transmit=True,
        )

        # Stop order — STP GTC (OCA group links them)
        oca_group = f"smc_{ticker}_{int(entry_price * 100)}"
        stop_order = StopOrder(
            action="SELL",
            totalQuantity=shares,
            stopPrice=round(stop_price, 2),
            tif="GTC",
            ocaGroup=oca_group,
            transmit=True,
        )

        entry_trade = self.ib.placeOrder(contract, entry_order)
        stop_trade = self.ib.placeOrder(contract, stop_order)

        logger.info(
            "Bracket order placed for %s: %d shares, entry=%.2f, stop=%.2f",
            ticker, shares, entry_price, stop_price,
        )
        return entry_trade, stop_trade

    async def place_market_sell(self, ticker: str, shares: int) -> Trade:
        """Submit a MKT sell order (used for RSI / time-stop exits)."""
        contract = self._stock_contract(ticker)
        await self.ib.qualifyContractsAsync(contract)
        order = MarketOrder(action="SELL", totalQuantity=shares)
        trade = self.ib.placeOrder(contract, order)
        logger.info("MKT SELL %s x %d", ticker, shares)
        return trade

    # -- kill switch ---------------------------------------------------------

    async def cancel_all_orders(self) -> None:
        """Cancel every open order and flatten all positions."""
        # Cancel open orders
        open_orders = self.ib.openOrders()
        for order in open_orders:
            self.ib.cancelOrder(order)
        logger.warning("Cancelled %d open orders", len(open_orders))

        await asyncio.sleep(1)  # give cancellations a moment

        # Flatten positions
        positions = await self.get_positions()
        for ticker, shares in positions.items():
            if shares > 0:
                await self.place_market_sell(ticker, shares)
            elif shares < 0:
                # Short cover (shouldn't happen in this strategy)
                contract = self._stock_contract(ticker)
                await self.ib.qualifyContractsAsync(contract)
                order = MarketOrder(action="BUY", totalQuantity=abs(shares))
                self.ib.placeOrder(contract, order)
        logger.warning("Flattened %d positions", len(positions))


if __name__ == "__main__":
    import sys

    from smc_trader.logger import setup_logging

    setup_logging()

    async def _demo() -> None:
        cfg = Config()
        broker = IBKRBroker(cfg)
        print(f"IBKRBroker created — target {cfg.ibkr_host}:{cfg.ibkr_port}")
        print("(not connecting in self-test — would need TWS/Gateway running)")
        print("Available methods:")
        for name in dir(broker):
            if not name.startswith("_"):
                print(f"  .{name}()")

    asyncio.run(_demo())
