"""Pre-market scanner for Opening Range Breakout candidates.

Uses IBKR reqScannerDataAsync to find top gappers, then enriches each
candidate with historical volume / ATR data and scores them.
"""

from __future__ import annotations

import logging
from typing import Any

from ib_async import IB, ScannerSubscription, Stock, TagValue

from orb_trader.config import ORBConfig

logger = logging.getLogger(__name__)


async def scan_gappers(ib: IB, config: ORBConfig) -> list[dict]:
    """Return a list of top gappers from IBKR scanner.

    Each entry contains ``'symbol'`` and ``'rank'``.
    """
    sub = ScannerSubscription(
        instrument="STK",
        locationCode="STK.US.MAJOR",
        scanCode="TOP_PERC_GAIN",
    )
    filter_options = [
        TagValue("changePercAbove", str(config.min_gap_pct)),
        TagValue("priceAbove", str(config.min_price)),
        TagValue("priceBelow", str(config.max_price)),
        TagValue("avgVolumeAbove", "500000"),
    ]

    try:
        scan_data = await ib.reqScannerDataAsync(sub, filter_options)
    except Exception as exc:
        logger.error("Scanner request failed: %s", exc)
        return []

    results: list[dict] = []
    for item in scan_data:
        symbol = item.contractDetails.contract.symbol
        results.append({"symbol": symbol, "rank": item.rank})
        logger.debug("Scanner hit: rank=%d symbol=%s", item.rank, symbol)

    logger.info("scan_gappers: %d candidates returned", len(results))
    return results


async def enrich_candidates(
    ib: IB,
    symbols: list[str],
    config: ORBConfig,
) -> list[dict]:
    """Fetch daily bars and pre-market data to enrich each candidate.

    Returns a list of dicts with keys:
    ``symbol``, ``avg_daily_vol``, ``atr14``, ``prev_range``,
    ``premarket_vol``, ``price``, ``gap_pct``.
    """
    enriched: list[dict] = []

    for symbol in symbols:
        contract = Stock(symbol, "SMART", "USD")
        try:
            await ib.qualifyContractsAsync(contract)
        except Exception as exc:
            logger.warning("Could not qualify %s: %s", symbol, exc)
            continue

        # --- 20 daily bars for ATR / avg volume ---
        try:
            daily_bars = await ib.reqHistoricalDataAsync(
                contract,
                endDateTime="",
                durationStr="1 M",
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=True,
                keepUpToDate=False,
            )
        except Exception as exc:
            logger.warning("Daily bars failed for %s: %s", symbol, exc)
            continue

        if len(daily_bars) < 2:
            logger.debug("Not enough daily bars for %s — skip", symbol)
            continue

        # Average daily volume (up to last 20 complete bars)
        recent = daily_bars[-20:] if len(daily_bars) >= 20 else daily_bars[:]
        avg_daily_vol = sum(b.volume for b in recent) / len(recent) if recent else 0.0

        # 14-day ATR
        bars_for_atr = daily_bars[-15:] if len(daily_bars) >= 15 else daily_bars[:]
        true_ranges: list[float] = []
        for i in range(1, len(bars_for_atr)):
            high = bars_for_atr[i].high
            low = bars_for_atr[i].low
            prev_close = bars_for_atr[i - 1].close
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            true_ranges.append(tr)
        atr14 = sum(true_ranges[-14:]) / len(true_ranges[-14:]) if true_ranges else 0.0

        # Yesterday's range
        prev_bar = daily_bars[-2] if len(daily_bars) >= 2 else daily_bars[-1]
        prev_range = prev_bar.high - prev_bar.low

        # Current / last price from most recent daily bar
        last_bar = daily_bars[-1]
        price = last_bar.close if last_bar.close else last_bar.open

        # Previous close for gap calculation
        prev_close_price = daily_bars[-2].close if len(daily_bars) >= 2 else price
        gap_pct = (
            ((price - prev_close_price) / prev_close_price * 100)
            if prev_close_price
            else 0.0
        )

        # --- Pre-market 5-min bars to estimate pre-market volume ---
        premarket_vol = 0.0
        try:
            pm_bars = await ib.reqHistoricalDataAsync(
                contract,
                endDateTime="",
                durationStr="1 D",
                barSizeSetting="5 mins",
                whatToShow="TRADES",
                useRTH=False,
                keepUpToDate=False,
            )
            # Sum bars before 9:30 ET
            from datetime import time as dtime
            from zoneinfo import ZoneInfo

            ET = ZoneInfo("US/Eastern")
            for bar in pm_bars:
                bar_dt = bar.date
                if hasattr(bar_dt, "astimezone"):
                    bar_t = bar_dt.astimezone(ET).time()
                else:
                    bar_t = bar_dt
                if bar_t < dtime(9, 30):
                    premarket_vol += bar.volume
        except Exception as exc:
            logger.debug("Pre-market bars failed for %s: %s", symbol, exc)

        enriched.append(
            {
                "symbol": symbol,
                "avg_daily_vol": avg_daily_vol,
                "atr14": atr14,
                "prev_range": prev_range,
                "premarket_vol": premarket_vol,
                "price": price,
                "gap_pct": gap_pct,
            }
        )
        logger.debug(
            "Enriched %s: price=%.2f gap=%.1f%% atr14=%.2f avg_vol=%.0f pm_vol=%.0f",
            symbol, price, gap_pct, atr14, avg_daily_vol, premarket_vol,
        )

    return enriched


def score_candidate(data: dict, config: ORBConfig) -> int:
    """Score a candidate 0-100 based on RVOL, gap, ATR, and range compression.

    Parameters
    ----------
    data:
        Enriched candidate dict as returned by ``enrich_candidates``.
    config:
        ORBConfig for threshold values.

    Returns
    -------
    int
        Score in [0, 100].
    """
    score = 0

    # --- RVOL (up to 30 pts) ---
    avg_daily_vol = data.get("avg_daily_vol", 0.0)
    premarket_vol = data.get("premarket_vol", 0.0)
    rvol = (
        premarket_vol / (avg_daily_vol * 0.05)
        if avg_daily_vol > 0
        else 0.0
    )
    if rvol >= 5:
        score += 30
    elif rvol >= 3:
        score += 20
    elif rvol >= 1.5:
        score += 10

    # --- Gap % (up to 25 pts) ---
    gap_pct = data.get("gap_pct", 0.0)
    if 2.0 <= gap_pct <= 6.0:
        score += 25
    elif 1.0 <= gap_pct < 2.0:
        score += 15
    elif gap_pct > 6.0:
        score += 10

    # --- ATR % (up to 15 pts) ---
    price = data.get("price", 0.0)
    atr14 = data.get("atr14", 0.0)
    atr_pct = (atr14 / price * 100) if price > 0 else 0.0
    if 3.0 <= atr_pct <= 8.0:
        score += 15
    elif 2.0 <= atr_pct < 3.0:
        score += 8

    # --- Previous range compression (up to 15 pts) ---
    prev_range = data.get("prev_range", 0.0)
    compression = (prev_range / atr14) if atr14 > 0 else 1.0
    if compression < 0.7:
        score += 15
    elif compression < 1.0:
        score += 10

    return score


async def build_watchlist(ib: IB, config: ORBConfig) -> list[str]:
    """Build the final ORB watchlist for the trading day.

    Steps:
    1. Scan for top gappers.
    2. Enrich each candidate with historical data.
    3. Score and filter (score >= 30).
    4. Return the top ``config.scanner_top_n`` symbols by score.
    """
    raw = await scan_gappers(ib, config)
    if not raw:
        logger.warning("No gapper candidates returned from scanner")
        return []

    symbols = [item["symbol"] for item in raw]
    enriched = await enrich_candidates(ib, symbols, config)

    # Apply gap and ATR filters, then score
    scored: list[tuple[int, str]] = []
    for data in enriched:
        gap_pct = data.get("gap_pct", 0.0)
        price = data.get("price", 0.0)
        atr14 = data.get("atr14", 0.0)
        atr_pct = (atr14 / price * 100) if price > 0 else 0.0

        # Hard filters
        if gap_pct < config.min_gap_pct or gap_pct > config.max_gap_pct:
            logger.debug("Filter gap: %s gap=%.1f%%", data["symbol"], gap_pct)
            continue
        if price < config.min_price or price > config.max_price:
            logger.debug("Filter price: %s price=%.2f", data["symbol"], price)
            continue
        if atr_pct < config.min_atr_pct or atr_pct > config.max_atr_pct:
            logger.debug("Filter ATR: %s atr_pct=%.1f%%", data["symbol"], atr_pct)
            continue

        s = score_candidate(data, config)
        if s >= 30:
            scored.append((s, data["symbol"]))
            logger.debug("Scored %s: %d", data["symbol"], s)
        else:
            logger.debug("Low score %s: %d — excluded", data["symbol"], s)

    # Sort descending by score, take top N
    scored.sort(key=lambda x: x[0], reverse=True)
    watchlist = [sym for _, sym in scored[: config.scanner_top_n]]

    logger.info(
        "build_watchlist: %d candidates qualified, top %d selected: %s",
        len(scored), len(watchlist), watchlist,
    )
    return watchlist
