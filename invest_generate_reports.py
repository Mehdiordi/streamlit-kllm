"""Generate `invest_*.csv` outputs from the two Revolut exports.

Usage:
  ./.venv/bin/python invest_generate_reports.py

Outputs are written into the `data/` folder.
"""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import argparse

import pandas as pd

import invest_processing as inv


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Folder containing the Revolut export CSV files (default: data)",
    )
    parser.add_argument(
        "--account-csv",
        default=None,
        help="Path to account-statement CSV (default: auto-detect latest)",
    )
    parser.add_argument(
        "--consolidated-csv",
        default=None,
        help="Path to consolidated_statement CSV (default: auto-detect latest)",
    )
    parser.add_argument(
        "--max-gap-days",
        type=float,
        default=10.0,
        help="Max allowed buy/exchange date gap (days)",
    )
    parser.add_argument(
        "--allow-negative-gap-days",
        type=float,
        default=1.0,
        help="Allow BUY to be earlier than exchange by up to N days (timezone/statement quirks)",
    )
    parser.add_argument(
        "--min-buy",
        type=float,
        default=100.0,
        help="Ignore BUY orders smaller than this amount when matching (filters reinvest interest)",
    )

    args = parser.parse_args()

    data_dir = Path(args.data_dir)

    account_csv = args.account_csv
    if not account_csv:
        # Reuse the existing helper in processing.py style by just scanning filenames.
        # We avoid importing the dashboard pipeline here.
        candidates = sorted([p for p in data_dir.glob("*.csv") if "account-statement" in p.name])
        if not candidates:
            raise FileNotFoundError(f"No account-statement CSV found in {data_dir}")
        account_csv = str(candidates[-1].as_posix())

    consolidated_csv = args.consolidated_csv
    if not consolidated_csv:
        consolidated_csv = inv.find_latest_consolidated_statement_csv(data_dir)

    invest_tx = inv.parse_consolidated_investment_statement(consolidated_csv)
    exchanges = inv.extract_dkk_exchanges_from_account_statement(account_csv)

    cfg = inv.MatchConfig(
        max_abs_day_gap=float(args.max_gap_days),
        allow_negative_day_gap=float(args.allow_negative_gap_days),
        min_buy_abs_value=float(args.min_buy),
    )

    matches, unmatched_ex, unmatched_buys = inv.match_exchanges_to_invest_buys(
        exchanges, invest_tx, config=cfg
    )

    # Write outputs
    out_orders = data_dir / "invest_orders_all.csv"
    out_exchanges = data_dir / "invest_dkk_exchanges.csv"
    out_matches = data_dir / "invest_exchange_buy_matches.csv"
    out_unmatched_ex = data_dir / "invest_exchange_unmatched.csv"
    out_unmatched_b = data_dir / "invest_buy_unmatched.csv"

    inv.write_csv(invest_tx, out_orders)
    inv.write_csv(exchanges, out_exchanges)
    inv.write_csv(matches, out_matches)
    inv.write_csv(unmatched_ex, out_unmatched_ex)
    inv.write_csv(unmatched_buys, out_unmatched_b)

    # Small console summary
    print("Inputs:")
    print(f"  account_csv: {account_csv}")
    print(f"  consolidated_csv: {consolidated_csv}")
    print("Config:")
    print(f"  {asdict(cfg)}")
    print("Outputs:")
    print(f"  {out_orders}")
    print(f"  {out_exchanges}")
    print(f"  {out_matches}")
    print(f"  {out_unmatched_ex}")
    print(f"  {out_unmatched_b}")

    if not matches.empty:
        print("\nMatched FX examples (DKK per CCY):")
        show_cols = [
            "exchange_exchange_completed_date",
            "exchange_to_currency",
            "exchange_from_amount",
            "buy_tx_datetime",
            "buy_value",
            "implied_rate_dkk_per_ccy",
            "day_gap",
        ]
        present = [c for c in show_cols if c in matches.columns]
        print(matches[present].head(12).to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
