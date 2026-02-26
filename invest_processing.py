"""Investment account statement parsing + reconciliation helpers.

Purpose
- Parse Revolut consolidated investment statement exports (multi-block CSV text)
- Extract funding-related BUY orders (e.g. Flexible Cash Funds)
- Extract DKK->X exchanges from the normal account statement
- Match exchange outflows to investment BUY orders to derive implied FX rates

This module is intentionally separate from the spending dashboard pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import csv
import re
from typing import Iterable, Optional

import pandas as pd


_CCY_SYMBOL_TO_CODE = {
    "£": "GBP",
    "$": "USD",
    "€": "EUR",
    "kr": "DKK",
}


def _strip_bom(s: str) -> str:
    return s.lstrip("\ufeff")


def find_latest_consolidated_statement_csv(search_dir: str | Path = "/Users/mehdiordikhani/Library/Mobile Documents/com~apple~Numbers/Documents") -> str:
    base = Path(search_dir)
    if not base.exists() or not base.is_dir():
        raise FileNotFoundError(f"Folder not found: {base}")

    candidates = [p for p in base.glob("*.csv") if "consolidated_statement" in p.name]
    if not candidates:
        raise FileNotFoundError(
            f"No CSV files containing 'consolidated_statement' in {base}"
        )

    date_re = re.compile(r"\d{4}-\d{2}-\d{2}")

    def sort_key(p: Path):
        dates = date_re.findall(p.name)
        parsed = [pd.to_datetime(d, errors="coerce").date() for d in dates]
        parsed = [d for d in parsed if d is not None and not pd.isna(d)]
        end_date = parsed[-1] if parsed else None
        start_date = parsed[0] if parsed else None
        mtime = p.stat().st_mtime
        end_dt = end_date or pd.Timestamp("1900-01-01").date()
        start_dt = start_date or pd.Timestamp("1900-01-01").date()
        return (end_dt, start_dt, mtime)

    best = sorted(candidates, key=sort_key, reverse=True)[0]
    return str(best.as_posix())


def extract_end_date_from_filename(csv_path: str | Path) -> Optional[pd.Timestamp]:
    """Extract the end date from consolidated statement filename.
    
    Example: 'consolidated_statement_2026-01-01_2026-02-24.csv' -> 2026-02-24
    """
    p = Path(csv_path)
    filename = p.name
    
    date_re = re.compile(r"\d{4}-\d{2}-\d{2}")
    dates = date_re.findall(filename)
    
    if not dates:
        return None
    
    # The end date is typically the last date in the filename
    end_date_str = dates[-1]
    try:
        return pd.to_datetime(end_date_str)
    except Exception:
        return None


def parse_investment_summary(csv_path: str | Path) -> pd.DataFrame:
    """Parse the Summary sections at the top of consolidated investment statement.
    
    Extracts:
    - Summary for Flexible Cash Funds - GBP
    - Summary for Flexible Cash Funds - USD  
    - Summary for Crypto
    
    Returns DataFrame with columns: section, description, amount, currency, value
    """
    rows = []
    
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()
    
    current_section = None
    in_summary = False
    
    for line in lines:
        line = _strip_bom(line.strip())
        
        # Detect section headers
        if line.startswith("Summary for"):
            current_section = line.replace("Summary for ", "").strip()
            in_summary = True
            continue
        
        # Stop processing when we hit transactions
        if line.startswith("Transactions for"):
            break
        
        if in_summary and current_section:
            # Skip empty lines
            if not line:
                continue
            
            # Skip header lines like "Description,Amount"
            if line in ["Description,Amount", "Sells summary,Amount"]:
                continue
            
            # Parse CSV line
            parts = line.split(",", 1)
            if len(parts) == 2:
                desc = parts[0].strip().strip('"')
                amount_str = parts[1].strip().strip('"')
                
                # Parse the amount
                value, currency = parse_money(amount_str)
                
                rows.append({
                    "section": current_section,
                    "description": desc,
                    "amount": amount_str,
                    "currency": currency,
                    "value": value
                })
    
    return pd.DataFrame(rows)


def save_investment_snapshot(
    snapshot_date: pd.Timestamp,
    summary_metrics: dict[str, float],
    history_file: str | Path = "data/investment_history.csv"
) -> None:
    """Save or update investment snapshot to history CSV.
    
    Args:
        snapshot_date: Date of the snapshot (from filename)
        summary_metrics: Dict of metric name -> DKK value
        history_file: Path to history CSV file
    """
    history_path = Path(history_file)
    
    # Load existing history if it exists
    if history_path.exists():
        history_df = pd.read_csv(history_path)
        history_df['date'] = pd.to_datetime(history_df['date'])
    else:
        history_df = pd.DataFrame(columns=['date'])
    
    # Create new row
    new_row = {'date': snapshot_date}
    new_row.update(summary_metrics)
    
    # Remove existing entry for this date if present
    history_df = history_df[history_df['date'] != snapshot_date]
    
    # Append new row
    history_df = pd.concat([history_df, pd.DataFrame([new_row])], ignore_index=True)
    
    # Sort by date
    history_df = history_df.sort_values('date').reset_index(drop=True)
    
    # Save
    history_path.parent.mkdir(parents=True, exist_ok=True)
    history_df.to_csv(history_path, index=False)


def load_investment_history(history_file: str | Path = "data/investment_history.csv") -> pd.DataFrame:
    """Load investment history from CSV.
    
    Returns DataFrame with columns: date, <metric1>, <metric2>, ...
    """
    history_path = Path(history_file)
    
    if not history_path.exists():
        return pd.DataFrame(columns=['date'])
    
    df = pd.read_csv(history_path)
    df['date'] = pd.to_datetime(df['date'])
    return df.sort_values('date').reset_index(drop=True)


def parse_money(value: object) -> tuple[Optional[float], Optional[str]]:
    """Parse money strings like '£405.55', '$15,233.98', '-£0.0854'.

    Returns (amount, currency_code). Currency may be None if unknown.
    """

    if value is None:
        return None, None

    s = str(value).strip()
    if not s:
        return None, None

    # Normalize unicode spaces used in exports.
    s = s.replace("\u202f", " ")  # narrow no-break space

    # Detect currency
    ccy = None
    for sym, code in _CCY_SYMBOL_TO_CODE.items():
        if sym in s:
            ccy = code
            break

    # Remove currency symbols/words
    cleaned = s
    cleaned = cleaned.replace("£", "").replace("$", "").replace("€", "")
    cleaned = cleaned.replace("DKK", "").replace("USD", "").replace("GBP", "")
    cleaned = cleaned.replace("EUR", "").replace("kr", "")
    cleaned = cleaned.strip()

    # Remove thousands separators
    cleaned = cleaned.replace(",", "")

    try:
        return float(cleaned), ccy
    except Exception:
        return None, ccy


def parse_consolidated_investment_statement(
    csv_path: str | Path,
) -> pd.DataFrame:
    """Parse the consolidated statement file into a single flat transaction table.

    The file contains multiple sections; we currently parse the blocks:
      Transactions for Flexible Cash Funds - <CCY>
      Transactions for Crypto

    Returns columns:
      - section (e.g. 'Flexible Cash Funds', 'Crypto')
      - currency (e.g. 'GBP', 'USD')
      - tx_datetime
      - description
      - value
      - raw_value
      - action (BUY/SELL/INTEREST/FEE/REINVEST/OTHER)
      - source_file
    """

    p = Path(csv_path)
    text = p.read_text(encoding="utf-8", errors="replace")
    lines = [_strip_bom(l) for l in text.splitlines()]

    rows: list[dict[str, object]] = []

    i = 0
    tx_re = re.compile(r"^Transactions for (.+?) - ([A-Z]{3})\s*$")

    while i < len(lines):
        header = lines[i].strip()

        m = tx_re.match(header)
        if m:
            section_raw = m.group(1).strip()
            currency = m.group(2).strip()

            section = section_raw
            i += 1
            if i >= len(lines):
                break

            col_line = lines[i]
            cols = next(csv.reader([col_line]))
            # Expect: Date,Description,Value,Price per share,Quantity per share
            i += 1

            while i < len(lines) and lines[i].strip():
                rec = next(csv.reader([lines[i]]))
                i += 1
                if len(rec) < 3:
                    continue

                dt_s = rec[0]
                desc = rec[1]
                raw_value = rec[2]

                tx_datetime = pd.to_datetime(dt_s, errors="coerce")
                value, parsed_ccy = parse_money(raw_value)

                action = _infer_action(desc)

                rows.append(
                    {
                        "section": section,
                        "currency": parsed_ccy or currency,
                        "tx_datetime": tx_datetime,
                        "description": desc,
                        "value": value,
                        "raw_value": raw_value,
                        "action": action,
                        "source_file": str(p.name),
                    }
                )

            # Skip blank line(s) after a block
            while i < len(lines) and not lines[i].strip():
                i += 1
            continue

        if header.strip() == "Transactions for Crypto":
            section = "Crypto"
            currency = None
            i += 1
            if i >= len(lines):
                break

            col_line = lines[i]
            cols = next(csv.reader([col_line]))
            i += 1

            while i < len(lines) and lines[i].strip():
                rec = next(csv.reader([lines[i]]))
                i += 1

                # Expect: Date acquired,Date sold,Token name,Qty,Cost basis,Gross proceeds,Gross PnL
                if len(rec) < 7:
                    continue

                date_acq = pd.to_datetime(rec[0], errors="coerce")
                date_sold = pd.to_datetime(rec[1], errors="coerce")
                token = rec[2]
                qty = rec[3]
                cost_basis, cost_ccy = parse_money(rec[4])
                proceeds, proceeds_ccy = parse_money(rec[5])
                pnl, pnl_ccy = parse_money(rec[6])

                currency = proceeds_ccy or cost_ccy or pnl_ccy

                rows.append(
                    {
                        "section": section,
                        "currency": currency,
                        "tx_datetime": date_sold,
                        "description": f"SELL {token} qty={qty}",
                        "value": proceeds,
                        "raw_value": rec[5],
                        "action": "SELL",
                        "source_file": str(p.name),
                        "date_acquired": date_acq,
                        "cost_basis": cost_basis,
                        "gross_pnl": pnl,
                    }
                )

            while i < len(lines) and not lines[i].strip():
                i += 1
            continue

        i += 1

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out["tx_datetime"] = pd.to_datetime(out["tx_datetime"], errors="coerce")
    out["currency"] = out.get("currency", pd.Series(dtype="object")).astype("object")
    out["description"] = out.get("description", pd.Series(dtype="object")).astype("object")
    out["action"] = out.get("action", pd.Series(dtype="object")).astype("object")
    out["value"] = pd.to_numeric(out.get("value"), errors="coerce")

    out = out.sort_values(["tx_datetime", "section", "currency", "description"], kind="stable")
    out = out.reset_index(drop=True)
    return out


def _infer_action(description: object) -> str:
    s = "" if description is None else str(description).strip()
    up = s.upper()

    if up.startswith("BUY "):
        return "BUY"
    if up.startswith("SELL "):
        return "SELL"

    if "INTEREST REINVESTED" in up:
        return "REINVEST"
    if "INTEREST" in up and "PAID" in up:
        return "INTEREST"
    if "SERVICE FEE" in up or "FEE" in up:
        return "FEE"

    return "OTHER"


def extract_dkk_exchanges_from_account_statement(
    account_csv_path: str | Path,
    only_completed: bool = True,
) -> pd.DataFrame:
    """Extract DKK outflows for rows like: Exchanged to GBP,-3500.00,DKK."""

    df = pd.read_csv(account_csv_path)
    if df.empty:
        return df

    # Normalize column names as in processing.py
    df = df.rename(columns={c: c.strip() for c in df.columns})

    # Parse datetimes
    for col in ["Completed Date", "Started Date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in ["Amount", "Fee", "Balance"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    out = df.copy()

    is_exchange = out.get("Type", "").astype(str).str.strip().eq("Exchange")
    is_dkk = out.get("Currency", "").astype(str).str.strip().str.upper().eq("DKK")
    desc = out.get("Description", pd.Series("", index=out.index)).astype(str)
    has_to = desc.str.match(r"^Exchanged\s+to\s+[A-Z]{3}\s*$", na=False)

    amount = pd.to_numeric(out.get("Amount"), errors="coerce")
    fee = pd.to_numeric(out.get("Fee"), errors="coerce").fillna(0)

    is_outflow = amount < 0

    state = out.get("State", pd.Series("", index=out.index)).astype(str)
    if only_completed:
        is_state_ok = state.str.strip().str.upper().eq("COMPLETED")
    else:
        is_state_ok = pd.Series(True, index=out.index)

    keep = is_exchange & is_dkk & has_to & is_outflow & is_state_ok
    out = out.loc[keep].copy()
    if out.empty:
        return pd.DataFrame(
            columns=[
                "exchange_completed_date",
                "exchange_description",
                "from_currency",
                "from_amount",
                "to_currency",
                "fee",
                "source_file",
            ]
        )

    out["to_currency"] = (
        out["Description"].astype(str).str.extract(r"^Exchanged\s+to\s+([A-Z]{3})\s*$")[0]
    )
    out["exchange_completed_date"] = out["Completed Date"]
    out["exchange_description"] = out["Description"]
    out["from_currency"] = "DKK"

    # amount_net for negative outflow: -(amount - abs(fee)) => positive DKK paid
    out["fee"] = fee.loc[out.index].astype(float)
    outflow_net = (amount.loc[out.index] - out["fee"].abs()).astype(float)
    out["from_amount"] = (-outflow_net).astype(float)

    out["source_file"] = str(Path(account_csv_path).name)

    keep_cols = [
        "exchange_completed_date",
        "exchange_description",
        "from_currency",
        "from_amount",
        "to_currency",
        "fee",
        "source_file",
    ]
    out = out[keep_cols].sort_values(["exchange_completed_date"], kind="stable").reset_index(
        drop=True
    )

    return out


def extract_exchange_pairs_from_account_statement(
    account_csv_path: str | Path,
    target_currencies: tuple[str, ...] = ("USD", "GBP"),
    only_completed: bool = True,
) -> pd.DataFrame:
    """Extract paired exchange legs for DKK -> {USD,GBP,...}.

    Revolut exports contain exchange legs as separate rows. For example:
      - Exchange ... Exchanged to GBP, -3500.00, Currency=DKK
      - Exchange ... Exchanged to GBP,  405.55,  Currency=GBP

    We group by (completed_date, started_date, description, state) and aggregate:
      - dkk_outflow: positive DKK paid (after fees)
      - foreign_inflow: positive foreign currency received (after fees)
      - implied_bank_rate_dkk_per_ccy = dkk_outflow / foreign_inflow
    """

    df = pd.read_csv(account_csv_path)
    if df.empty:
        return pd.DataFrame()

    df = df.rename(columns={c: c.strip() for c in df.columns})

    for col in ["Completed Date", "Started Date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in ["Amount", "Fee", "Balance"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    typ = df.get("Type", pd.Series("", index=df.index)).astype(str).str.strip()
    desc = df.get("Description", pd.Series("", index=df.index)).astype(str)
    ccy = df.get("Currency", pd.Series("", index=df.index)).astype(str).str.upper().str.strip()
    state = df.get("State", pd.Series("", index=df.index)).astype(str).str.upper().str.strip()

    is_exchange = typ.eq("Exchange")
    has_to = desc.str.match(r"^Exchanged\s+to\s+[A-Z]{3}\s*$", na=False)

    if only_completed:
        is_state_ok = state.eq("COMPLETED")
    else:
        is_state_ok = pd.Series(True, index=df.index)

    df = df.loc[is_exchange & has_to & is_state_ok].copy()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "completed_date",
                "started_date",
                "description",
                "to_currency",
                "dkk_outflow",
                "foreign_inflow",
                "implied_bank_rate_dkk_per_ccy",
                "source_file",
            ]
        )

    df["to_currency"] = df["Description"].astype(str).str.extract(
        r"^Exchanged\s+to\s+([A-Z]{3})\s*$"
    )[0]

    targets = {str(x).upper().strip() for x in target_currencies if str(x).strip()}
    df = df.loc[df["to_currency"].astype(str).str.upper().str.strip().isin(targets)].copy()
    if df.empty:
        return pd.DataFrame(
            columns=[
                "completed_date",
                "started_date",
                "description",
                "to_currency",
                "dkk_outflow",
                "foreign_inflow",
                "implied_bank_rate_dkk_per_ccy",
                "source_file",
            ]
        )

    fee = pd.to_numeric(df.get("Fee"), errors="coerce").fillna(0.0)
    amt = pd.to_numeric(df.get("Amount"), errors="coerce")
    df["amount_net"] = (amt - fee.abs()).astype(float)

    grp_cols = ["Completed Date", "Started Date", "Description", "State", "to_currency"]

    def agg_one(g: pd.DataFrame) -> pd.Series:
        cc = g.get("Currency", pd.Series("", index=g.index)).astype(str).str.upper().str.strip()
        net = pd.to_numeric(g.get("amount_net"), errors="coerce")

        dkk_net = net.loc[cc.eq("DKK")]
        foreign_net = net.loc[cc.eq(str(g["to_currency"].iloc[0]).upper().strip())]

        dkk_outflow = float((-dkk_net[dkk_net < 0]).sum()) if not dkk_net.empty else 0.0
        foreign_inflow = float((foreign_net[foreign_net > 0]).sum()) if not foreign_net.empty else 0.0

        implied = None
        if foreign_inflow and foreign_inflow > 0 and dkk_outflow and dkk_outflow > 0:
            implied = dkk_outflow / foreign_inflow

        return pd.Series(
            {
                "dkk_outflow": dkk_outflow,
                "foreign_inflow": foreign_inflow,
                "implied_bank_rate_dkk_per_ccy": implied,
            }
        )

    agg = df.groupby(grp_cols, dropna=False).apply(agg_one).reset_index()
    agg = agg.rename(
        columns={
            "Completed Date": "completed_date",
            "Started Date": "started_date",
            "Description": "description",
            "State": "state",
        }
    )
    agg["source_file"] = str(Path(account_csv_path).name)

    # Keep only rows where we actually have a foreign inflow leg.
    agg["foreign_inflow"] = pd.to_numeric(agg.get("foreign_inflow"), errors="coerce")
    agg = agg.loc[agg["foreign_inflow"].notna() & (agg["foreign_inflow"] > 0)].copy()

    agg["completed_date"] = pd.to_datetime(agg["completed_date"], errors="coerce")
    agg = agg.sort_values(["completed_date", "to_currency"], kind="stable").reset_index(drop=True)
    return agg


@dataclass(frozen=True)
class MatchConfig:
    max_abs_day_gap: float = 10.0
    allow_negative_day_gap: float = 1.0
    skip_buy_penalty: float = 0.3
    # Exchanges should be allowed to remain unmatched if no plausible BUY exists.
    skip_exchange_penalty: float = 5.0
    min_buy_abs_value: float = 100.0
    rate_deviation_weight: float = 6.0
    max_rate_log_deviation: float = 0.7


def match_exchanges_to_invest_buys(
    exchanges: pd.DataFrame,
    invest_tx: pd.DataFrame,
    config: MatchConfig = MatchConfig(),
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Match each DKK->CCY exchange to an investment BUY order in that CCY.

    Returns (matches_df, unmatched_exchanges_df, unmatched_buys_df).

    Matching is done per currency using an order-preserving dynamic program:
    exchanges and buys are treated as sequences and aligned to minimize time gaps,
    while allowing skipping buys (e.g. reinvested interest) cheaply.
    """

    if exchanges.empty:
        return (
            pd.DataFrame(),
            exchanges.copy(),
            invest_tx.loc[invest_tx.get("action").astype(str).str.upper().eq("BUY")].copy(),
        )

    tx = invest_tx.copy()
    tx["tx_datetime"] = pd.to_datetime(tx["tx_datetime"], errors="coerce")
    tx["value"] = pd.to_numeric(tx["value"], errors="coerce")
    tx["action"] = tx.get("action", "").astype(str)
    tx["currency"] = tx.get("currency", "").astype(str).str.upper().str.strip()

    buys = tx.loc[tx["action"].str.upper().eq("BUY")].copy()
    buys = buys.loc[buys["value"].abs() >= float(config.min_buy_abs_value)].copy()

    ex = exchanges.copy()
    ex["exchange_completed_date"] = pd.to_datetime(ex["exchange_completed_date"], errors="coerce")
    ex["to_currency"] = ex.get("to_currency", "").astype(str).str.upper().str.strip()
    ex["from_amount"] = pd.to_numeric(ex.get("from_amount"), errors="coerce")

    match_rows: list[dict[str, object]] = []
    unmatched_exchange_rows: list[pd.DataFrame] = []
    unmatched_buy_rows: list[pd.DataFrame] = []

    for ccy in sorted(set(ex["to_currency"].dropna().unique().tolist())):
        ex_c = ex.loc[ex["to_currency"].eq(ccy)].sort_values(
            ["exchange_completed_date"], kind="stable"
        )
        buy_c = buys.loc[buys["currency"].eq(ccy)].sort_values(["tx_datetime"], kind="stable")

        if ex_c.empty:
            continue

        if buy_c.empty:
            unmatched_exchange_rows.append(ex_c)
            continue

        mapping, unmatched_ex_idx, unmatched_buy_idx = _align_sequences_iterative_rate(
            ex_c, buy_c, config
        )

        # Emit matched rows
        for ex_i, buy_j, day_gap in mapping:
            ex_row = ex_c.iloc[ex_i]
            buy_row = buy_c.iloc[buy_j]
            buy_amount = float(buy_row["value"]) if pd.notna(buy_row["value"]) else None

            implied_rate = None
            if buy_amount and buy_amount != 0 and pd.notna(ex_row.get("from_amount")):
                implied_rate = float(ex_row["from_amount"]) / float(buy_amount)

            match_rows.append(
                {
                    **{f"exchange_{k}": ex_row.get(k) for k in ex_row.index},
                    **{f"buy_{k}": buy_row.get(k) for k in buy_row.index},
                    "day_gap": day_gap,
                    "implied_rate_dkk_per_ccy": implied_rate,
                }
            )

        if unmatched_ex_idx:
            unmatched_exchange_rows.append(ex_c.iloc[unmatched_ex_idx].copy())
        if unmatched_buy_idx:
            unmatched_buy_rows.append(buy_c.iloc[unmatched_buy_idx].copy())

    matches_df = pd.DataFrame(match_rows)
    unmatched_ex_df = (
        pd.concat(unmatched_exchange_rows, ignore_index=True) if unmatched_exchange_rows else ex.iloc[0:0].copy()
    )
    unmatched_buy_df = (
        pd.concat(unmatched_buy_rows, ignore_index=True) if unmatched_buy_rows else buys.iloc[0:0].copy()
    )

    if not matches_df.empty:
        matches_df["exchange_exchange_completed_date"] = pd.to_datetime(
            matches_df["exchange_exchange_completed_date"], errors="coerce"
        )
        matches_df["buy_tx_datetime"] = pd.to_datetime(matches_df["buy_tx_datetime"], errors="coerce")
        matches_df = matches_df.sort_values(
            ["exchange_to_currency", "exchange_exchange_completed_date", "buy_tx_datetime"],
            kind="stable",
        ).reset_index(drop=True)

    unmatched_ex_df = unmatched_ex_df.sort_values(["exchange_completed_date"], kind="stable").reset_index(drop=True)
    unmatched_buy_df = unmatched_buy_df.sort_values(["tx_datetime"], kind="stable").reset_index(drop=True)

    return matches_df, unmatched_ex_df, unmatched_buy_df


def _align_sequences_iterative_rate(
    exchanges: pd.DataFrame,
    buys: pd.DataFrame,
    config: MatchConfig,
) -> tuple[list[tuple[int, int, float]], list[int], list[int]]:
    """Align sequences, then refine using rate consistency.

    1) Run a gap-only alignment to get an initial set of matches.
    2) Estimate a typical implied FX rate (median in log-space).
    3) Re-run alignment with a penalty for implied rates far from the median.

    This avoids forcing absurd matches when an exchange isn't actually funding the
    investment account (or when the consolidated statement contains extra BUYs like
    reinvested interest).
    """

    mapping, _unmatched_ex, _unmatched_buy = _align_sequences_core(
        exchanges, buys, config, rate_center=None
    )

    center = _estimate_rate_center(exchanges, buys, mapping)
    if center is None or not (center > 0):
        return mapping, _unmatched_ex, _unmatched_buy

    mapping2, unmatched_ex2, unmatched_buy2 = _align_sequences_core(
        exchanges, buys, config, rate_center=center
    )
    return mapping2, unmatched_ex2, unmatched_buy2


def _align_sequences_core(
    exchanges: pd.DataFrame,
    buys: pd.DataFrame,
    config: MatchConfig,
    rate_center: float | None,
) -> tuple[list[tuple[int, int, float]], list[int], list[int]]:
    ex_dt = pd.to_datetime(exchanges["exchange_completed_date"], errors="coerce").tolist()
    buy_dt = pd.to_datetime(buys["tx_datetime"], errors="coerce").tolist()
    ex_amt = pd.to_numeric(exchanges.get("from_amount"), errors="coerce").tolist()
    buy_val = pd.to_numeric(buys.get("value"), errors="coerce").tolist()

    n = len(ex_dt)
    m = len(buy_dt)

    inf = 1e18
    dp_cost = [[inf] * (m + 1) for _ in range(n + 1)]
    back: list[list[tuple[int, int, str] | None]] = [[None] * (m + 1) for _ in range(n + 1)]
    dp_cost[0][0] = 0.0

    def day_gap(i: int, j: int) -> float:
        if pd.isna(ex_dt[i]) or pd.isna(buy_dt[j]):
            return float("inf")
        delta = buy_dt[j] - ex_dt[i]
        return float(delta.total_seconds() / 86400.0)

    def implied_rate(i: int, j: int) -> float | None:
        a = ex_amt[i]
        b = buy_val[j]
        if a is None or b is None:
            return None
        if pd.isna(a) or pd.isna(b):
            return None
        if float(b) == 0:
            return None
        r = float(a) / float(b)
        if r <= 0:
            return None
        return r

    def rate_penalty(r: float | None) -> float:
        if rate_center is None:
            return 0.0
        if r is None:
            return float("inf")
        import math

        log_dev = abs(math.log(r / float(rate_center)))
        if log_dev > float(config.max_rate_log_deviation):
            return float("inf")
        return float(config.rate_deviation_weight) * log_dev

    for i in range(n + 1):
        for j in range(m + 1):
            cur = dp_cost[i][j]
            if cur >= inf:
                continue

            if i < n and j < m:
                gap = day_gap(i, j)
                if (
                    gap != float("inf")
                    and gap <= float(config.max_abs_day_gap)
                    and gap >= -float(config.allow_negative_day_gap)
                ):
                    rp = rate_penalty(implied_rate(i, j))
                    if rp != float("inf"):
                        cost = cur + abs(gap) + rp
                        if cost < dp_cost[i + 1][j + 1]:
                            dp_cost[i + 1][j + 1] = cost
                            back[i + 1][j + 1] = (i, j, "M")

            if j < m:
                cost = cur + float(config.skip_buy_penalty)
                if cost < dp_cost[i][j + 1]:
                    dp_cost[i][j + 1] = cost
                    back[i][j + 1] = (i, j, "SB")

            if i < n:
                cost = cur + float(config.skip_exchange_penalty)
                if cost < dp_cost[i + 1][j]:
                    dp_cost[i + 1][j] = cost
                    back[i + 1][j] = (i, j, "SE")

    end_j = min(range(m + 1), key=lambda jj: dp_cost[n][jj])

    mapping: list[tuple[int, int, float]] = []
    matched_ex: set[int] = set()
    matched_buy: set[int] = set()

    i, j = n, end_j
    while i > 0 or j > 0:
        bp = back[i][j]
        if bp is None:
            break
        pi, pj, action = bp
        if action == "M":
            gap = day_gap(pi, pj)
            mapping.append((pi, pj, gap))
            matched_ex.add(pi)
            matched_buy.add(pj)
        i, j = pi, pj

    mapping.reverse()

    unmatched_ex = [k for k in range(n) if k not in matched_ex]
    unmatched_buy = [k for k in range(m) if k not in matched_buy]
    return mapping, unmatched_ex, unmatched_buy


def _estimate_rate_center(
    exchanges: pd.DataFrame,
    buys: pd.DataFrame,
    mapping: list[tuple[int, int, float]],
) -> float | None:
    """Robustly estimate a typical implied rate for this currency from matches."""

    if not mapping:
        return None

    ex_amt = pd.to_numeric(exchanges.get("from_amount"), errors="coerce").tolist()
    buy_val = pd.to_numeric(buys.get("value"), errors="coerce").tolist()

    rates: list[float] = []
    for ex_i, buy_j, _gap in mapping:
        a = ex_amt[ex_i]
        b = buy_val[buy_j]
        if a is None or b is None:
            continue
        if pd.isna(a) or pd.isna(b) or float(b) == 0:
            continue
        r = float(a) / float(b)
        if r > 0:
            rates.append(r)

    if len(rates) < 2:
        return rates[0] if rates else None

    import math
    import numpy as np

    logs = np.array([math.log(r) for r in rates], dtype=float)
    med = float(np.median(logs))
    mad = float(np.median(np.abs(logs - med)))
    if mad <= 0:
        return float(math.exp(med))

    # Filter extreme outliers before taking final center.
    keep = np.abs(logs - med) <= (3.5 * mad)
    if keep.sum() == 0:
        return float(math.exp(med))

    center = float(math.exp(float(np.median(logs[keep]))))
    return center


def write_csv(df: pd.DataFrame, path: str | Path) -> str:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False)
    return str(p.as_posix())


def normalize_paths(paths: Iterable[str | Path]) -> list[str]:
    return [str(Path(p).as_posix()) for p in paths]
