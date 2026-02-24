"""Revolut CSV processing utilities.

This module mirrors the notebook pipeline in 01_visualize_df.ipynb:
- load exactly one Revolut export CSV
- normalize schema + parse types
- classify transactions (expense/income/refund)
- categorize expenses using config.py rules
- convert amount_net to DKK on completed_date using historical FX
- prepare monthly expense-by-category series for plotting
"""

from __future__ import annotations

from dataclasses import dataclass
import importlib
import logging
import re
import time
import unicodedata
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

import config

logger = logging.getLogger(__name__)


def to_snake(name: str) -> str:
    name = str(name).strip()
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_").lower()


def load_revolut_csv(csv_path: str) -> pd.DataFrame:
    """Load exactly one Revolut export CSV."""
    return pd.read_csv(csv_path)


def normalize_revolut_df(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    df.columns = [to_snake(c) for c in df.columns]

    # Revolut export: keep original Type in `sub_type`
    if "type" in df.columns and "sub_type" not in df.columns:
        df = df.rename(columns={"type": "sub_type"})

    # Dates (Completed Date drives FX conversion)
    for col in ["completed_date", "started_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Numeric columns
    for col in ["amount", "fee", "balance"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Net amount includes fee (fee is typically positive in Revolut exports)
    df["fee"] = pd.to_numeric(df.get("fee", 0), errors="coerce").fillna(0)
    df["amount_net"] = (
        pd.to_numeric(df.get("amount", np.nan), errors="coerce") - df["fee"].abs()
    )

    # Drop leftovers from older runs / schema changes
    df = df.drop(columns=["expense_category", "conversion_date"], errors="ignore")

    return df


def classify_type(frame: pd.DataFrame) -> pd.Series:
    """Return high-level transaction type: expense/income/refund/NA."""
    out = pd.Series(pd.NA, index=frame.index, dtype="object")

    sub_type = (
        frame.get("sub_type", pd.Series("", index=frame.index, dtype="object"))
        .astype(str)
        .str.strip()
    )
    desc = frame.get("description", pd.Series("", index=frame.index, dtype="object")).astype(
        str
    )

    # Expense rule
    out.loc[sub_type.eq("Card Payment")] = "expense"

    # Income rules (override expense if both ever match)
    is_income = desc.str.contains("BETTERAI LLC", case=False, na=False) | desc.str.contains(
        "paypal", case=False, na=False
    )
    out.loc[is_income] = "income"

    # Refund rule: if ANY column contains 'refund' (case-insensitive), mark as refund (overrides everything).
    has_refund = frame.astype(str).apply(
        lambda col: col.str.contains("refund", case=False, na=False)
    )
    is_refund = has_refund.any(axis=1)
    out.loc[is_refund] = "refund"

    return out


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = unicodedata.normalize("NFKC", str(value)).casefold()
    text = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2212]", "-", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _compiled_expense_rules() -> List[Tuple[str, List[str]]]:
    # Reload so edits to config.py apply without restarting the process.
    cfg = importlib.reload(config)
    return [
        (category_name, [normalize_text(k) for k in keywords])
        for category_name, keywords in cfg.EXPENSE_CATEGORY_RULES
    ]


def categorize_expenses(df: pd.DataFrame) -> pd.DataFrame:
    if "type" not in df.columns:
        raise ValueError("Missing required column: type")

    compiled_rules = _compiled_expense_rules()
    cfg = importlib.reload(config)

    def category_from_description(description: object) -> str:
        text = normalize_text(description)
        for category_name, keywords in compiled_rules:
            if any(k and k in text for k in keywords):
                return category_name
        return cfg.DEFAULT_EXPENSE_CATEGORY

    out = df.copy()
    is_expense = out["type"].astype(str).str.casefold().eq("expense")

    out["category"] = pd.NA
    if "description" in out.columns:
        out.loc[is_expense, "category"] = out.loc[is_expense, "description"].apply(
            category_from_description
        )

    return out


def fx_rate_on_date(
    date: pd.Timestamp,
    from_ccy: str,
    to_ccy: str = "DKK",
    max_backtrack_days: int = 10,
    _cache: Dict[Tuple[str, str, str], Tuple[Optional[float], Optional[pd.Timestamp]]] = {},
) -> Tuple[Optional[float], Optional[pd.Timestamp]]:
    """Returns (rate, used_date).

    Uses ECB daily rates via frankfurter.app and backtracks for weekends/holidays.
    """

    from_ccy = str(from_ccy).upper().strip()
    to_ccy = str(to_ccy).upper().strip()

    if not from_ccy or from_ccy == to_ccy:
        used = pd.Timestamp(date).date() if not pd.isna(date) else None
        return 1.0, used

    if pd.isna(date):
        return None, None

    d = pd.Timestamp(date).date()
    for attempt in range(max_backtrack_days + 1):
        key = (str(d), from_ccy, to_ccy)
        if key in _cache:
            return _cache[key]

        url = f"https://api.frankfurter.app/{d}?from={from_ccy}&to={to_ccy}"
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                rate = float(data["rates"][to_ccy])
                api_date_str = data.get("date")
                used_date = pd.to_datetime(api_date_str).date() if api_date_str else d
                _cache[key] = (rate, used_date)
                return rate, used_date
        except Exception:
            logger.warning(
                f"Failed to fetch FX rate for {from_ccy}->{to_ccy} on {d} "
                f"(attempt {attempt+1}/{max_backtrack_days+1})"
            )

        d = (pd.Timestamp(d) - pd.Timedelta(days=1)).date()
        time.sleep(0.05)

    return None, None


def convert_to_dkk(df: pd.DataFrame) -> pd.DataFrame:
    """Add amount_dkk + conversion_rate for income/expense/refund rows with completed_date."""

    out = df.copy()
    out = out.drop(columns=["conversion_date"], errors="ignore")
    out["amount_dkk"] = pd.NA
    out["conversion_rate"] = pd.NA

    required_cols = ["type", "amount_net", "currency", "completed_date"]
    for col in required_cols:
        if col not in out.columns:
            return out

    mask = (
        out["type"].isin(["income", "expense", "refund"])
        & out["amount_net"].notna()
        & out["currency"].notna()
        & out["completed_date"].notna()
    )

    if not mask.any():
        return out

    ccy = out.loc[mask, "currency"].astype(str).str.upper().str.strip()
    dt = pd.to_datetime(out.loc[mask, "completed_date"], errors="coerce").dt.normalize()
    amt = pd.to_numeric(out.loc[mask, "amount_net"], errors="coerce")

    rate = pd.Series(1.0, index=ccy.index, dtype="float")
    need = ccy.ne("DKK") & dt.notna()

    pairs = pd.DataFrame({"dt": dt[need], "ccy": ccy[need]}).drop_duplicates()

    pair_to_rate: Dict[Tuple[object, str], Optional[float]] = {}
    for row in pairs.itertuples(index=False):
        fx, _used = fx_rate_on_date(row.dt, row.ccy, "DKK")
        pair_to_rate[(row.dt.date(), row.ccy)] = fx

    rate.loc[need] = [
        pair_to_rate.get((d.date(), c), np.nan) for d, c in zip(dt[need], ccy[need])
    ]

    out.loc[mask, "conversion_rate"] = rate
    out.loc[mask, "amount_dkk"] = amt * rate

    return out


@dataclass(frozen=True)
class PreparedData:
    df: pd.DataFrame
    totals_by_month: pd.DataFrame
    spend_by_month_category: pd.DataFrame
    other_expenses: pd.DataFrame


def prepare_data_for_plotting(csv_path: str) -> PreparedData:
    """End-to-end prep used by Streamlit plotting."""

    raw = load_revolut_csv(csv_path)
    df = normalize_revolut_df(raw)
    df["type"] = classify_type(df)
    df = categorize_expenses(df)
    df = convert_to_dkk(df)

    base = df.copy()
    base = base[base["completed_date"].notna()].copy() if "completed_date" in base.columns else base
    base["amount_dkk"] = pd.to_numeric(base.get("amount_dkk"), errors="coerce")
    base = base[base["amount_dkk"].notna()].copy()

    if base.empty or "completed_date" not in base.columns:
        totals = pd.DataFrame(columns=["expense", "income", "refund"])
        by_month_cat = pd.DataFrame(columns=["month", "category", "spend_dkk"])
    else:
        base["month"] = base["completed_date"].dt.to_period("M").astype(str)

        types = ["expense", "income", "refund"]
        totals = (
            base[base["type"].isin(types)]
            .assign(value_dkk=lambda x: x["amount_dkk"].abs())
            .groupby(["month", "type"])["value_dkk"]
            .sum()
            .unstack(fill_value=0.0)
        )
        for t in types:
            if t not in totals.columns:
                totals[t] = 0.0

        exp = base[base["type"].astype(str).str.casefold().eq("expense")].copy()
        exp = exp[exp.get("category").notna()].copy() if "category" in exp.columns else exp.iloc[0:0]
        by_month_cat = (
            exp.assign(spend_dkk=lambda x: x["amount_dkk"].abs())
            .groupby(["month", "category"])["spend_dkk"]
            .sum()
            .reset_index()
        )

    other_df = df.copy()
    if "type" in other_df.columns:
        other_df = other_df[other_df["type"].astype(str).str.casefold().eq("expense")].copy()
    if "category" in other_df.columns:
        other_df = other_df[other_df["category"].astype(str).eq("Other")].copy()

    if not other_df.empty:
        other_df["amount_dkk"] = pd.to_numeric(other_df.get("amount_dkk"), errors="coerce")
        other_df["spend_dkk"] = other_df["amount_dkk"].abs()
        sort_cols = [c for c in ["spend_dkk", "completed_date"] if c in other_df.columns]
        if sort_cols:
            other_df = other_df.sort_values(sort_cols, ascending=[False] + [True] * (len(sort_cols) - 1))

    return PreparedData(
        df=df,
        totals_by_month=totals,
        spend_by_month_category=by_month_cat,
        other_expenses=other_df,
    )
