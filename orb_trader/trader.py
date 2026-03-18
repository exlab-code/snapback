"""Main ORBTrader class.

Connects to IBKR via ib_async, subscribes to 1-min live bars for each
watchlist symbol, and executes bracket orders on ORB breakout signals.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from ib_async import IB, MarketOrder, Stock
import ib_async.util as util

from smc_trader.risk import calculate_shares
from smc_trader.logger import alert
from orb_trader.config import ORBConfig
from orb_trader.signals import ORBState, process_bar
from orb_trader.scanner import build_watchlist

ET = ZoneInfo("US/Eastern")
logger = logging.getLogger(__name__)


class ORBTrader:
    """Event-driven Opening Range Breakout trader."""

    def __init__(self, config: ORBConfig) -> None:
        self.config = config
        self.ib = IB()
        self.states: dict[str, ORBState] = {}
        self.bar_lists: dict[str, object] = {}        # symbol -> BarDataList
        self.open_positions: dict[str, dict] = {}     # symbol -> {entry, stop, shares, ...}
        self._connected = False

    # -------------------------------------------------------------------------
    # Connection
    # -------------------------------------------------------------------------

    async def connect(self) -> None:
        """Connect to TWS / IB Gateway."""
        await self.ib.connectAsync(
            host=self.config.ibkr_host,
            port=self.config.ibkr_port,
            clientId=self.config.ibkr_client_id,
        )
        self.ib.reqMarketDataType(1)  # live data
        self._connected = True
        logger.info(
            "ORB trader connected to IBKR on %s:%d",
            self.config.ibkr_host,
            self.config.ibkr_port,
        )

    async def disconnect(self) -> None:
        """Disconnect from IBKR."""
        if self._connected:
            self.ib.disconnect()
            self._connected = False

    # -------------------------------------------------------------------------
    # Pre-market scan
    # -------------------------------------------------------------------------

    async def run_premarket_scan(self) -> None:
        """Called at 9:15 ET — build today's watchlist and subscribe to bars."""
        logger.info("=== ORB pre-market scan ===")
        symbols = await build_watchlist(self.ib, self.config)
        logger.info("Watchlist: %s", symbols)
        alert(
            f"ORB watchlist: {', '.join(symbols)}",
            token=self.config.telegram_token,
            chat_id=self.config.telegram_chat_id,
        )

        # Reset state for the new day
        self.states = {}
        self.bar_lists = {}

        for symbol in symbols:
            await self._subscribe_bars(symbol)

    # -------------------------------------------------------------------------
    # Bar subscription
    # -------------------------------------------------------------------------

    async def _subscribe_bars(self, symbol: str) -> None:
        """Qualify contract and start a live 1-min bar stream."""
        contract = Stock(symbol, "SMART", "USD")
        await self.ib.qualifyContractsAsync(contract)

        bars = await self.ib.reqHistoricalDataAsync(
            contract,
            endDateTime="",
            durationStr="1 D",
            barSizeSetting="1 min",
            whatToShow="TRADES",
            useRTH=True,
            keepUpToDate=True,
        )

        self.states[symbol] = ORBState(symbol=symbol)
        self.bar_lists[symbol] = bars
        bars.updateEvent += self._on_bar_update
        logger.info("Subscribed to 1-min bars for %s", symbol)

    # -------------------------------------------------------------------------
    # Bar event handler
    # -------------------------------------------------------------------------

    def _on_bar_update(self, bars, hasNewBar: bool) -> None:
        """Fired by ib_async whenever a bar in the stream is updated."""
        if not hasNewBar:
            return

        symbol = bars.contract.symbol
        state = self.states.get(symbol)
        if not state:
            return

        # The last bar in the list is still forming; use the second-to-last
        if len(bars) < 2:
            return
        closed_bar = bars[-2]

        bar_dt = closed_bar.date
        if hasattr(bar_dt, "astimezone"):
            bar_time = bar_dt.astimezone(ET).time()
        else:
            bar_time = bar_dt

        # Only process during RTH
        if bar_time < time(9, 30) or bar_time >= time(16, 0):
            return

        signal = process_bar(state, closed_bar, self.config)

        if signal == "LONG" and len(self.open_positions) < self.config.max_positions:
            asyncio.create_task(
                self._execute_long(symbol, bars.contract, closed_bar, state)
            )

    # -------------------------------------------------------------------------
    # Order execution
    # -------------------------------------------------------------------------

    async def _execute_long(self, symbol: str, contract, bar, state: ORBState) -> None:
        """Place a bracket order for a long ORB breakout."""
        if state.traded_today:
            return
        state.traded_today = True

        entry_price = bar.close
        stop_price = state.or_low  # stop at bottom of OR

        try:
            equity = (
                self.config.account_size
                if self.config.account_size > 0
                else await self._get_equity()
            )
        except Exception:
            equity = self.config.account_size or 10_000.0

        stop_pct = (
            (entry_price - stop_price) / entry_price
            if entry_price > stop_price
            else 0.05
        )

        shares = calculate_shares(
            equity=equity,
            entry_price=entry_price,
            stop_pct=stop_pct,
            risk_pct=self.config.risk_per_trade,
            max_pos_pct=self.config.max_position_pct,
        )

        if shares <= 0:
            logger.warning("Zero shares calculated for %s — skipping", symbol)
            state.traded_today = False  # allow retry if sizing improves
            return

        target_price = round(entry_price + state.or_width, 2)
        stop_price = round(stop_price, 2)

        bracket = self.ib.bracketOrder(
            "BUY",
            shares,
            round(entry_price + 0.02, 2),  # limit price slightly above signal close
            target_price,
            stop_price,
        )

        # Override entry to market order for immediate fill
        bracket[0].orderType = "MKT"
        bracket[0].tif = "DAY"
        bracket[1].tif = "DAY"   # profit target
        bracket[2].orderType = "STP"
        bracket[2].tif = "DAY"   # stop loss

        for order in bracket:
            self.ib.placeOrder(contract, order)

        self.open_positions[symbol] = {
            "entry": entry_price,
            "stop": stop_price,
            "target": target_price,
            "shares": shares,
            "entry_time": datetime.now(ET),
        }

        logger.info(
            "ORB LONG %s: %d shares @ %.2f, stop %.2f, target %.2f",
            symbol, shares, entry_price, stop_price, target_price,
        )
        alert(
            f"ORB BUY {shares} {symbol} @ {entry_price:.2f} "
            f"stop {stop_price:.2f} target {target_price:.2f}",
            token=self.config.telegram_token,
            chat_id=self.config.telegram_chat_id,
        )

    # -------------------------------------------------------------------------
    # EOD flatten
    # -------------------------------------------------------------------------

    async def eod_flatten(self) -> None:
        """Called at 15:45 ET — market sell all open ORB positions."""
        logger.info("=== ORB EOD flatten ===")
        positions = self.ib.positions()
        orb_symbols = set(self.states.keys())

        for pos in positions:
            sym = pos.contract.symbol
            if sym not in orb_symbols or pos.position <= 0:
                continue
            order = MarketOrder("SELL", int(pos.position))
            order.tif = "MOC"  # market on close
            self.ib.placeOrder(pos.contract, order)
            logger.info("EOD flatten: SELL %d %s", int(pos.position), sym)
            alert(
                f"ORB EOD SELL {int(pos.position)} {sym}",
                token=self.config.telegram_token,
                chat_id=self.config.telegram_chat_id,
            )

        # Cancel any remaining open child orders for ORB symbols
        for trade in self.ib.openTrades():
            if trade.contract.symbol in orb_symbols:
                self.ib.cancelOrder(trade.order)

        # Unsubscribe from bar streams
        for bars in self.bar_lists.values():
            self.ib.cancelHistoricalData(bars)
        self.bar_lists.clear()
        self.states.clear()
        self.open_positions.clear()

    # -------------------------------------------------------------------------
    # Account helpers
    # -------------------------------------------------------------------------

    async def _get_equity(self) -> float:
        """Return NetLiquidation in USD from the account summary."""
        summary = await self.ib.accountSummaryAsync()
        for item in summary:
            if item.tag == "NetLiquidation" and item.currency == "USD":
                return float(item.value)
        return 10_000.0
