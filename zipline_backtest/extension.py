"""Zipline extension — registers the smc_csvdir bundle.

Zipline loads this file automatically if placed at
~/.zipline/extension.py  OR if ZIPLINE_EXTENSION_MODS points to it.

We register a csvdir bundle that reads from the local CSV exports.
Uses a custom ingest function to set country_code='US' on the exchange,
which is required by the Pipeline engine.
"""

from __future__ import annotations

import os
import numpy as np
import pandas as pd
from zipline.data.bundles import register
from zipline.data.bundles.csvdir import CSVDIRBundle
from zipline.utils.calendar_utils import register_calendar_alias

CSV_BUNDLE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "csv_bundle"
)

try:
    register_calendar_alias("SMC_CSVDIR", "NYSE")
except Exception:
    pass


class _SMCBundle(CSVDIRBundle):
    """CSVDIRBundle subclass that sets country_code='US' on the exchange."""

    def ingest(
        self,
        environ,
        asset_db_writer,
        minute_bar_writer,
        daily_bar_writer,
        adjustment_writer,
        calendar,
        start_session,
        end_session,
        cache,
        show_progress,
        output_dir,
    ):
        from zipline.data.bundles.csvdir import _pricing_iter
        from zipline.utils.cli import maybe_show_progress

        csvdir = self.csvdir or environ.get("CSVDIR")
        ddir = os.path.join(csvdir, "daily")

        symbols = sorted(
            item.split(".csv")[0]
            for item in os.listdir(ddir)
            if item.endswith(".csv")
        )

        dtype = [
            ("start_date", "datetime64[ns]"),
            ("end_date", "datetime64[ns]"),
            ("auto_close_date", "datetime64[ns]"),
            ("symbol", "object"),
        ]
        metadata = pd.DataFrame(np.empty(len(symbols), dtype=dtype))

        divs_splits = {
            "divs": pd.DataFrame(
                columns=[
                    "sid", "amount", "ex_date", "record_date",
                    "declared_date", "pay_date",
                ]
            ),
            "splits": pd.DataFrame(columns=["sid", "ratio", "effective_date"]),
        }

        daily_bar_writer.write(
            _pricing_iter(ddir, symbols, metadata, divs_splits, show_progress),
            show_progress=show_progress,
        )

        metadata["exchange"] = "SMC_CSVDIR"

        # Explicitly set country_code='US' so the Pipeline engine can find assets
        exchanges = pd.DataFrame({
            "exchange": ["SMC_CSVDIR"],
            "canonical_name": ["SMC_CSVDIR"],
            "country_code": ["US"],
        })

        asset_db_writer.write(equities=metadata, exchanges=exchanges)

        divs_splits["divs"]["sid"] = divs_splits["divs"]["sid"].astype(int)
        divs_splits["splits"]["sid"] = divs_splits["splits"]["sid"].astype(int)
        adjustment_writer.write(
            splits=divs_splits["splits"],
            dividends=divs_splits["divs"],
        )


register(
    "smc_csvdir",
    _SMCBundle(["daily"], CSV_BUNDLE_DIR).ingest,
    calendar_name="XNYS",
    start_session=pd.Timestamp("2003-01-02"),
    end_session=pd.Timestamp("2026-12-31"),
)
