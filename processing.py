"""Revolut CSV processing utilities.

This module mirrors the notebook pipeline in 01_visualize_df.ipynb:
- load exactly one Revolut export CSV
- normalize schema + parse types
- classify transactions (expense/income/refund)
- categorize expenses using expense_categories.yml rules
- convert amount_net to DKK on completed_date using historical FX
- prepare monthly expense-by-category series for plotting
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date as Date
from pathlib import Path
import calendar
import csv
from datetime import datetime
import logging
import re
import shutil
import unicodedata
from uuid import uuid4
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

import fx_cache

logger = logging.getLogger(__name__)

DEFAULT_EXPENSE_CATEGORY = "Other"
EXPENSE_CATEGORY_MAP_PATH = Path(__file__).with_name("expense_categories.yml")

MANUAL_EXPENSES_FILENAME = "manual_expenses.csv"
MANUAL_EXPENSES_BACKUP_FILENAME = "manual_expenses.backup.csv"
MANUAL_EXTERNAL_SUFFIX = "-External"


_expense_config_cache: dict[Path, tuple[float, dict[str, str], dict[int, float]]] = {}


def _month_key_to_number(key: str) -> int | None:
    k = normalize_text(key)
    if not k:
        return None

    # Accept "January", "jan", etc (case/space insensitive via normalize_text)
    k_compact = k.replace(" ", "")

    for i in range(1, 13):
        full = normalize_text(calendar.month_name[i]).replace(" ", "")
        abbr = normalize_text(calendar.month_abbr[i]).replace(" ", "")
        if k_compact == full or (abbr and k_compact == abbr):
            return i

    return None


def _to_float_maybe(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return float(s)
        except Exception:
            return None
    return None


def _find_latest_statement_csv(search_dir: str, name_contains: str) -> str:
    """Pick the most recent statement CSV whose filename contains the given token."""

    base = Path(search_dir)
    if not base.exists() or not base.is_dir():
        raise FileNotFoundError(f"Folder not found: {base}")

    candidates = [p for p in base.glob("*.csv") if name_contains in p.name]
    if not candidates:
        raise FileNotFoundError(f"No CSV files containing '{name_contains}' in {base}")

    date_re = re.compile(r"\d{4}-\d{2}-\d{2}")

    def sort_key(p: Path):
        dates = date_re.findall(p.name)
        parsed = [pd.to_datetime(d, errors="coerce").date() for d in dates]
        parsed = [d for d in parsed if d is not None and not pd.isna(d)]
        end_date = parsed[-1] if parsed else None
        start_date = parsed[0] if parsed else None
        mtime = p.stat().st_mtime
        # None-safe sorting: use very old date when missing
        end_dt = end_date or pd.Timestamp("1900-01-01").date()
        start_dt = start_date or pd.Timestamp("1900-01-01").date()
        return (end_dt, start_dt, mtime)

    best = sorted(candidates, key=sort_key, reverse=True)[0]
    return str(best.as_posix())


def find_latest_account_statement_csv(search_dir: str = "/Users/mehdiordikhani/Library/Mobile Documents/com~apple~Numbers/Documents") -> str:
    """Pick the most recent Revolut export CSV containing 'account-statement'.

    Preference order:
    1) Newest date found in filename (typically the end date in Revolut exports)
    2) Newest file modified time as a fallback
    """

    return _find_latest_statement_csv(search_dir, "account-statement")


def find_latest_savings_statement_csv(search_dir: str = "/Users/mehdiordikhani/Library/Mobile Documents/com~apple~Numbers/Documents") -> str:
    """Pick the most recent savings statement CSV containing 'savings-statement'."""

    return _find_latest_statement_csv(search_dir, "savings-statement")


def cleanup_outdated_account_statement_csvs(
    search_dir: str = "data",
    keep_path: str | None = None,
    prefix: str = "account-statement",
) -> list[str]:
    """Remove older account-statement CSVs from search_dir.

    Only deletes files inside search_dir whose names start with `prefix` and end with `.csv`.
    Safety guard: never deletes MANUAL_EXPENSES_FILENAME.
    Returns a list of deleted file paths.
    """

    base = Path(search_dir)
    if not base.exists() or not base.is_dir():
        return []

    keep_resolved = None
    if keep_path:
        try:
            keep_resolved = Path(keep_path).resolve()
        except Exception:
            keep_resolved = None

    deleted: list[str] = []
    for p in base.glob("*.csv"):
        # Hard safety guard for manual data persistence.
        if p.name == MANUAL_EXPENSES_FILENAME:
            continue
        if not p.name.startswith(prefix):
            continue
        if keep_resolved is not None:
            try:
                if p.resolve() == keep_resolved:
                    continue
            except Exception:
                # If resolve fails, fall back to string compare.
                if str(p) == keep_path:
                    continue

        try:
            p.unlink()
            deleted.append(str(p.as_posix()))
        except Exception as e:
            logger.warning(f"Failed to delete outdated CSV {p}: {e}")

    return deleted


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
    
    # Use started_date as fallback for PENDING transactions without completed_date
    if "completed_date" in df.columns and "started_date" in df.columns:
        df["completed_date"] = df["completed_date"].fillna(df["started_date"])

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


def get_all_categories(path: str | Path | None = None) -> list[str]:
    """Return sorted list of all unique categories from expense_categories.yml."""
    p = Path(path) if path is not None else EXPENSE_CATEGORY_MAP_PATH
    compiled = load_expense_category_map(p)
    return sorted({str(category).strip() for _keyword, category in compiled if str(category).strip()})


def add_category_mapping(keyword: str, category: str, path: str | Path | None = None) -> None:
    """Add a new keyword -> category mapping to expense_categories.yml."""
    p = Path(path) if path is not None else EXPENSE_CATEGORY_MAP_PATH
    if not p.exists():
        raise FileNotFoundError(f"Expense categories file not found: {p}")

    keyword_text = str(keyword).strip()
    category_text = str(category).strip()
    if not keyword_text or not category_text:
        return

    current_mapping, _ = _load_expense_config_file(p)
    if keyword_text in current_mapping and str(current_mapping[keyword_text]).strip() == category_text:
        return

    escaped_key = keyword_text.replace("\\", "\\\\").replace('"', '\\"')
    escaped_category = category_text.replace("\\", "\\\\").replace('"', '\\"')

    # Keep comments/order by appending a new mapping line instead of rewriting the whole file.
    with p.open("a", encoding="utf-8") as f:
        f.write(f'\n"{escaped_key}": "{escaped_category}"\n')

    global _category_cache
    _category_cache.clear()
    global _expense_config_cache
    _expense_config_cache.clear()


def successful_transaction_mask(frame: pd.DataFrame) -> pd.Series:
    """Return True for rows that should be included in spend/income calculations.

    Rules:
    - Manual rows are always considered successful.
    - If `state` is present, only explicit successful states are included.
    - If `state` is missing/blank, keep the row (backward-compatible fallback).
    """

    mask = pd.Series(True, index=frame.index, dtype="bool")

    source = frame.get("source", pd.Series("", index=frame.index, dtype="object"))
    is_manual = source.astype(str).str.casefold().eq("manual")

    state = frame.get("state", pd.Series("", index=frame.index, dtype="object"))
    state_norm = state.astype(str).str.upper().str.strip()
    has_state = state_norm.ne("") & state_norm.ne("NAN")

    success_states = {
        "COMPLETED",
        "SETTLED",
        "EXECUTED",
    }
    failure_states = {
        "REVERTED",
        "FAILED",
        "DECLINED",
        "CANCELLED",
        "CANCELED",
        "REJECTED",
        "REVERSED",
        "EXPIRED",
    }

    is_success_state = state_norm.isin(success_states)
    is_failure_state = state_norm.isin(failure_states)

    # For rows with an explicit state, require success and block known failures.
    mask.loc[has_state] = is_success_state.loc[has_state] & ~is_failure_state.loc[has_state]

    # Manual rows do not carry provider state and should always be kept.
    mask.loc[is_manual] = True
    return mask


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = unicodedata.normalize("NFKC", str(value)).casefold()
    text = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2212]", "-", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_keyword(value: object) -> str:
    """Normalize a keyword used for substring rules.

    Unlike `normalize_text`, this preserves leading/trailing whitespace.
    This allows rule authors to intentionally use spaces for crude word-boundary
    matching (e.g. "bar " should not match "lebara").

        Matching behavior:
        - If a keyword has leading/trailing spaces, those spaces are treated as intentional
            and matching is done against the spaced-normalized description only.
        - Otherwise, matching is whitespace-insensitive: both the spaced form and a
            space-stripped form are checked.
    """

    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""

    text = unicodedata.normalize("NFKC", str(value)).casefold()
    text = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2212]", "-", text)
    # Collapse internal whitespace, but keep any intentional leading/trailing spaces.
    text = re.sub(r"\s+", " ", text)
    return text


def _matches_keyword(keyword_norm: str, text_norm: str, text_compact: str) -> bool:
    """Return True if keyword matches description under 'exact' rules.

    - Uses normalized (casefolded) strings.
    - Checks spaced substring match.
    - For keywords without intentional boundary spaces, also checks a compact form
      where spaces are removed to tolerate missing/extra whitespace.
    """

    if not keyword_norm:
        return False

    if keyword_norm in text_norm:
        return True

    if keyword_norm.startswith(" ") or keyword_norm.endswith(" "):
        return False

    kw_compact = keyword_norm.replace(" ", "")
    if not kw_compact:
        return False

    return kw_compact in text_compact


def _parse_simple_yaml_mapping(text: str) -> dict[str, str]:
    """Parse a small subset of YAML used by expense_categories.yml.

    Supports lines like:
      "netto": "Groceries"
      netto: Groceries

    Ignores blank lines and lines starting with '#'.
    """

    out: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        # tolerate trailing commas (people may copy JSON-ish snippets)
        if line.endswith(","):
            line = line[:-1].rstrip()

        if ":" not in line:
            continue

        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()

        def unquote(s: str) -> str:
            if len(s) >= 2 and ((s[0] == s[-1]) and s[0] in ("\"", "'")):
                return s[1:-1]
            return s

        key = unquote(key)
        value = unquote(value)
        if key and value:
            out[key] = value

    return out


def _load_expense_config_file(path: Path) -> tuple[dict[str, str], dict[int, float]]:
    """Load (keyword->category mapping, month_number->limit_dkk) from YAML.

    File format supports mixing both:
    - Category rules: "netto": "Groceries"
    - Monthly limits: January: 23000

    Uses PyYAML if installed; otherwise falls back to a small built-in mapping parser.
    """

    raw = path.read_text(encoding="utf-8")

    data: object | None = None
    try:
        import yaml  # type: ignore

        data = yaml.safe_load(raw)
    except Exception:
        data = None

    if not isinstance(data, dict):
        # Fallback parser returns string values
        data = _parse_simple_yaml_mapping(raw)

    categories: dict[str, str] = {}
    monthly_limits: dict[int, float] = {}

    for k, v in data.items():
        if k is None or v is None:
            continue

        # Monthly limit?
        month_num = _month_key_to_number(str(k))
        if month_num is not None:
            limit = _to_float_maybe(v)
            if limit is not None:
                monthly_limits[month_num] = float(limit)
            continue

        # Category mapping
        if isinstance(v, (str, int, float)):
            categories[str(k)] = str(v)

    return categories, monthly_limits


_category_cache: dict[Path, tuple[float, list[tuple[str, str]]]] = {}


def load_expense_category_map(path: str | Path | None = None) -> list[tuple[str, str]]:
    """Return compiled expense category mapping as [(keyword_norm, category)].

    - Keywords are normalized with `normalize_keyword` (preserves outer spaces).
    - Matching should be done against `normalize_text(description)`.
    - Order is longest-key-first to keep matching deterministic.
    """

    p = Path(path) if path is not None else EXPENSE_CATEGORY_MAP_PATH
    if not p.exists():
        raise FileNotFoundError(
            f"Missing expense category map: {p}. Expected a YAML mapping file like '\"netto\": \"Groceries\"'."
        )

    mtime = p.stat().st_mtime
    cached = _category_cache.get(p)
    if cached is not None and cached[0] == mtime:
        return cached[1]

    mapping, _monthly_limits = _load_expense_config_file(p)

    compiled: list[tuple[str, str]] = []
    for raw_key, raw_cat in mapping.items():
        key = normalize_keyword(raw_key)
        cat = str(raw_cat).strip()
        if not key or not cat:
            continue
        compiled.append((key, cat))

    compiled.sort(key=lambda kv: (len(kv[0]), kv[0]), reverse=True)

    _category_cache[p] = (mtime, compiled)
    return compiled


def load_monthly_limits(path: str | Path | None = None) -> dict[int, float]:
    """Load monthly expense limits (DKK) keyed by month number (1-12)."""

    p = Path(path) if path is not None else EXPENSE_CATEGORY_MAP_PATH
    if not p.exists():
        return {}

    mtime = p.stat().st_mtime
    cached = _expense_config_cache.get(p)
    if cached is not None and cached[0] == mtime:
        return dict(cached[2])

    categories, monthly_limits = _load_expense_config_file(p)
    _expense_config_cache[p] = (mtime, categories, monthly_limits)
    return dict(monthly_limits)


def explain_expense_category(description: object) -> tuple[str, str | None]:
    """Return (category, matched_keyword) for a description.

    Intended for debugging/trust-building when a classification looks wrong.
    """

    text = normalize_text(description)
    text_compact = text.replace(" ", "")
    compiled = load_expense_category_map()

    for keyword, category in compiled:
        if _matches_keyword(keyword, text, text_compact):
            return category, keyword

    return DEFAULT_EXPENSE_CATEGORY, None


def categorize_expenses(df: pd.DataFrame) -> pd.DataFrame:
    if "type" not in df.columns:
        raise ValueError("Missing required column: type")

    compiled = load_expense_category_map()

    def category_from_description(description: object) -> str:
        text = normalize_text(description)
        text_compact = text.replace(" ", "")
        for keyword, category in compiled:
            if _matches_keyword(keyword, text, text_compact):
                return category
        return DEFAULT_EXPENSE_CATEGORY

    out = df.copy()
    is_expense = out["type"].astype(str).str.casefold().eq("expense")

    if "category" not in out.columns:
        out["category"] = pd.NA

    if "description" in out.columns:
        to_fill = is_expense & out["category"].isna()
        out.loc[to_fill, "category"] = out.loc[to_fill, "description"].apply(
            category_from_description
        )

    return out


def manual_expenses_path(data_dir: str | Path = "data") -> Path:
    return Path(data_dir) / MANUAL_EXPENSES_FILENAME


def manual_expenses_backup_path(data_dir: str | Path = "data") -> Path:
    return Path(data_dir) / MANUAL_EXPENSES_BACKUP_FILENAME


def _restore_manual_expenses_from_backup(data_dir: str | Path = "data") -> None:
    """Restore primary manual expenses file from backup if it was removed."""

    primary = manual_expenses_path(data_dir)
    backup = manual_expenses_backup_path(data_dir)
    if primary.exists() or not backup.exists():
        return
    try:
        primary.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(backup, primary)
        logger.warning("Restored manual expenses file from backup: %s", primary)
    except Exception as e:
        logger.warning("Failed to restore manual expenses backup: %s", e)


def _sync_manual_expenses_backup(data_dir: str | Path = "data") -> None:
    """Keep a local backup copy of manual expenses for accidental-deletion recovery."""

    primary = manual_expenses_path(data_dir)
    backup = manual_expenses_backup_path(data_dir)
    if not primary.exists():
        return
    try:
        backup.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(primary, backup)
    except Exception as e:
        logger.warning("Failed to update manual expenses backup: %s", e)


def _ensure_external_suffix(description: object) -> str:
    s = "" if description is None else str(description)
    s = s.strip()
    if not s:
        return s
    if s.casefold().endswith(MANUAL_EXTERNAL_SUFFIX.casefold()):
        return s
    return s + MANUAL_EXTERNAL_SUFFIX


def load_manual_expenses(data_dir: str | Path = "data") -> pd.DataFrame:
    """Load manually-entered expenses from a persistent CSV.

    Expected columns (flexible):
    - completed_date (required)
    - description (required)
    - amount_net OR amount_dkk (required)
    - currency (optional; defaults to DKK)
    - category (optional)

    Returned rows are normalized to the main pipeline schema with:
    - type = 'expense'
    - sub_type = 'Manual'
    - fee = 0
    - source = 'manual'
    """

    _restore_manual_expenses_from_backup(data_dir)
    p = manual_expenses_path(data_dir)
    if not p.exists():
        return pd.DataFrame()

    try:
        raw = pd.read_csv(p)
    except Exception:
        return pd.DataFrame()

    if raw.empty:
        return pd.DataFrame()

    _sync_manual_expenses_backup(data_dir)

    df = raw.copy()
    # Normalize column names to snake_case to be forgiving
    df.columns = [to_snake(c) for c in df.columns]

    if "completed_date" not in df.columns or "description" not in df.columns:
        return pd.DataFrame()

    df["completed_date"] = pd.to_datetime(df["completed_date"], errors="coerce")
    df["description"] = df["description"].where(df["description"].notna(), "")
    df["description"] = df["description"].astype(str).map(_ensure_external_suffix)

    # Accept either amount_net or amount_dkk
    if "amount_net" not in df.columns:
        if "amount_dkk" in df.columns:
            df["amount_net"] = df["amount_dkk"]
        elif "amount" in df.columns:
            df["amount_net"] = df["amount"]
        else:
            return pd.DataFrame()

    df["amount_net"] = pd.to_numeric(df["amount_net"], errors="coerce")

    # Manual entries are expenses: store as negative net amount for consistency.
    df.loc[df["amount_net"].notna(), "amount_net"] = -df.loc[df["amount_net"].notna(), "amount_net"].abs()

    if "currency" not in df.columns:
        df["currency"] = "DKK"
    df["currency"] = df["currency"].astype(str).str.upper().str.strip().replace({"": "DKK"})
    df.loc[df["currency"].isna(), "currency"] = "DKK"

    # Manual expenses should not be keyword-categorized; trust the provided category.
    # If missing/blank, default to DEFAULT_EXPENSE_CATEGORY.
    if "category" in df.columns:
        df["category"] = df["category"].where(df["category"].notna(), "")
        df["category"] = df["category"].astype(str).map(lambda s: s.strip())
        df.loc[df["category"].eq(""), "category"] = DEFAULT_EXPENSE_CATEGORY
    else:
        df["category"] = DEFAULT_EXPENSE_CATEGORY

    # Fill required/expected columns for the rest of the pipeline
    df["type"] = "expense"
    df["sub_type"] = df.get("sub_type", "Manual")
    df["fee"] = 0.0
    df["source"] = "manual"

    keep_cols = [
        c
        for c in [
            "completed_date",
            "started_date",
            "sub_type",
            "description",
            "amount_net",
            "fee",
            "currency",
            "type",
            "category",
            "source",
        ]
        if c in df.columns
    ]
    df = df[keep_cols].copy()
    df = df[df["completed_date"].notna() & df["amount_net"].notna()].copy()
    return df


def append_manual_expense(
    *,
    data_dir: str | Path = "data",
    completed_date: Date,
    description: str,
    amount_dkk: float,
    category: str | None = None,
) -> Path:
    """Append one manual expense to the persistent CSV and return the path."""

    p = manual_expenses_path(data_dir)
    p.parent.mkdir(parents=True, exist_ok=True)

    row = {
        "id": str(uuid4()),
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "completed_date": str(completed_date),
        "description": _ensure_external_suffix(description),
        # Store as positive spend in file; loader converts to negative amount_net
        "amount_dkk": float(abs(amount_dkk)),
        "currency": "DKK",
        "category": (str(category).strip() if category else ""),
    }

    write_header = not p.exists() or p.stat().st_size == 0
    with p.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if write_header:
            w.writeheader()
        w.writerow(row)

    _sync_manual_expenses_backup(data_dir)

    return p


def fx_rate_on_date(
    date: pd.Timestamp,
    from_ccy: str,
    to_ccy: str = "DKK",
    max_backtrack_days: int = 10,
    _cache: Dict[Tuple[str, str, str], Tuple[Optional[float], Optional[pd.Timestamp]]] = {},
) -> Tuple[Optional[float], Optional[pd.Timestamp]]:
    """Backwards-compatible wrapper around fx_cache.fx_rate_on_date."""

    return fx_cache.fx_rate_on_date(
        date=date,
        from_ccy=from_ccy,
        to_ccy=to_ccy,
        max_backtrack_days=max_backtrack_days,
        _cache=_cache,
    )


def convert_to_dkk(
    df: pd.DataFrame,
    fx_data_dir: str | Path = "data",
    fx_cache_currencies: Iterable[str] = fx_cache.FX_CACHE_CURRENCIES,
    fx_cache_start_date: Date = fx_cache.FX_CACHE_START_DATE,
    to_ccy: str = fx_cache.FX_CACHE_TO_CCY,
) -> pd.DataFrame:
    """Add amount_dkk + conversion_rate for income/expense/refund rows with completed_date.

    Fast path:
    - Uses local FX cache CSVs for USD/EUR/GBP->DKK (stored under fx_data_dir)

    Fallback path:
    - For other currencies, uses the per-date frankfurter endpoint with backtracking.
    """

    out = df.copy()
    out = out.drop(columns=["conversion_date"], errors="ignore")
    out["amount_dkk"] = pd.NA
    out["conversion_rate"] = pd.NA

    required_cols = ["type", "amount_net", "currency", "completed_date"]
    for col in required_cols:
        if col not in out.columns:
            return out

    success_mask = successful_transaction_mask(out)
    mask = (
        out["type"].isin(["income", "expense", "refund"])
        & out["amount_net"].notna()
        & out["currency"].notna()
        & out["completed_date"].notna()
        & success_mask
    )

    if not mask.any():
        return out

    ccy = out.loc[mask, "currency"].astype(str).str.upper().str.strip()
    dt = pd.to_datetime(out.loc[mask, "completed_date"], errors="coerce").dt.normalize()
    amt = pd.to_numeric(out.loc[mask, "amount_net"], errors="coerce")

    rate = pd.Series(1.0, index=ccy.index, dtype="float")
    need = ccy.ne(to_ccy) & dt.notna()

    # Ensure required local cache files exist (first run blocks until created).
    fx_cache_set = {str(c).upper().strip() for c in fx_cache_currencies}
    if fx_cache_set:
        fx_cache.ensure_fx_cache_files(
            data_dir=fx_data_dir,
            currencies=fx_cache_set,
            start_date=fx_cache_start_date,
            to_ccy=to_ccy,
        )

    cached_need = need & ccy.isin(list(fx_cache_set))
    if bool(cached_need.any()):
        for from_ccy in fx_cache_set:
            idx = cached_need & ccy.eq(from_ccy)
            if not bool(idx.any()):
                continue

            s = fx_cache.load_fx_cache_series(from_ccy, data_dir=fx_data_dir, to_ccy=to_ccy)
            if s.empty:
                continue

            aligned = s.reindex(dt[idx])
            rate.loc[idx] = aligned.to_numpy(dtype="float")

    # Fallback for non-cached currencies
    api_need = need & ~ccy.isin(list(fx_cache_set))
    if bool(api_need.any()):
        pairs = pd.DataFrame({"dt": dt[api_need], "ccy": ccy[api_need]}).drop_duplicates()

        pair_to_rate: Dict[Tuple[object, str], Optional[float]] = {}
        for row in pairs.itertuples(index=False):
            fx, _used = fx_rate_on_date(row.dt, row.ccy, to_ccy)
            pair_to_rate[(row.dt.date(), row.ccy)] = fx

        rate.loc[api_need] = [
            pair_to_rate.get((d.date(), c), np.nan) for d, c in zip(dt[api_need], ccy[api_need])
        ]

    out.loc[mask, "conversion_rate"] = rate
    out.loc[mask, "amount_dkk"] = amt * rate

    return out


def refund_cross_month_summary(base: pd.DataFrame) -> dict[str, dict[str, object]]:
    """Return per-month details of refunds that offset expenses in earlier months.

    Months with no cross-month refunds are omitted. Example::

        {
            "2026-06": {
                "amount": 328.0,
                "items": [
                    {"description": "Boozt", "from_month": "2026-05", "amount": 328.0},
                ],
            }
        }
    """
    if base.empty:
        return {}

    allocations = debug_refund_allocations(base, verbose=False)
    if allocations.empty:
        return {}

    allocations["refund_date"] = pd.to_datetime(allocations["refund_date"])
    allocations["matched_expense_date"] = pd.to_datetime(allocations["matched_expense_date"])
    allocations["refund_month"] = allocations["refund_date"].dt.to_period("M").astype(str)
    allocations["expense_month"] = allocations["matched_expense_date"].dt.to_period("M").astype(str)
    cross = allocations[allocations["refund_month"] != allocations["expense_month"]].copy()
    if cross.empty:
        return {}

    grouped = (
        cross.groupby(["refund_month", "expense_month", "refund_description"], dropna=False)
        .agg(amount=("allocation_amount", "sum"))
        .reset_index()
        .sort_values(["refund_month", "amount"], ascending=[True, False])
    )

    summary: dict[str, dict[str, object]] = {}
    for refund_month, g in grouped.groupby("refund_month", sort=False):
        items = [
            {
                "description": str(row.refund_description),
                "from_month": str(row.expense_month),
                "amount": float(row.amount),
            }
            for row in g.itertuples(index=False)
        ]
        summary[str(refund_month)] = {
            "amount": float(g["amount"].sum()),
            "items": items,
        }
    return summary


def debug_refund_allocations(base: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
    """DEBUG: Show refund-to-expense allocations.
    
    Returns a DataFrame with columns:
    - refund_date, refund_desc, refund_amount
    - matched_expense_date, matched_expense_amount, allocation_amount
    """
    if base.empty:
        return pd.DataFrame()

    exp = base[base.get("type", "").astype(str).str.casefold().eq("expense")].copy()
    if exp.empty:
        return pd.DataFrame()

    exp_amount = pd.to_numeric(exp.get("amount_dkk"), errors="coerce").abs()
    exp = exp.loc[exp_amount.notna()].copy()
    if exp.empty:
        return pd.DataFrame()

    exp["_remaining_spend"] = exp_amount.loc[exp.index].astype(float)
    exp["_desc_norm"] = exp.get("description", pd.Series("", index=exp.index)).astype(str).map(normalize_text)
    exp["_completed"] = pd.to_datetime(exp.get("completed_date"), errors="coerce")

    ref = base[base.get("type", "").astype(str).str.casefold().eq("refund")].copy()
    if ref.empty:
        return pd.DataFrame()

    ref["_refund_amt"] = pd.to_numeric(ref.get("amount_dkk"), errors="coerce").abs()
    ref["_desc_norm"] = ref.get("description", pd.Series("", index=ref.index)).astype(str).map(normalize_text)
    ref["_completed"] = pd.to_datetime(ref.get("completed_date"), errors="coerce")
    ref = ref.loc[
        ref["_refund_amt"].notna()
        & ref["_completed"].notna()
        & ref["_desc_norm"].ne("")
    ].copy()

    if ref.empty:
        return pd.DataFrame()

    ref = ref.sort_values(["_completed"], ascending=[True])

    allocations = []
    for _idx, refund in ref.iterrows():
        remaining_refund = float(refund["_refund_amt"])
        if remaining_refund <= 0:
            continue

        eligible = exp[
            exp["_desc_norm"].eq(refund["_desc_norm"])
            & exp["_completed"].notna()
            & exp["_completed"].le(refund["_completed"])
            & exp["_remaining_spend"].gt(0)
        ]
        if eligible.empty:
            continue

        eligible = eligible.sort_values(["_completed"], ascending=[False])

        for idx in eligible.index.tolist():
            if remaining_refund <= 0:
                break
            cur = float(exp.at[idx, "_remaining_spend"])
            if cur <= 0:
                continue
            used = min(cur, remaining_refund)
            exp.at[idx, "_remaining_spend"] = cur - used
            remaining_refund -= used
            
            allocations.append({
                "refund_date": refund["_completed"],
                "refund_description": refund.get("description", ""),
                "refund_amount": float(refund["_refund_amt"]),
                "matched_expense_date": exp.at[idx, "_completed"],
                "matched_expense_amount": cur,
                "allocation_amount": used,
            })

    result = pd.DataFrame(allocations)
    if verbose and not result.empty:
        print("\n=== DEBUG: Refund-to-Expense Allocations ===")
        print(result.to_string(index=False))
        print()
    return result


def _expense_spend_after_refunds(base: pd.DataFrame) -> pd.Series:
    """Return per-expense spend after allocating refunds to prior matching expenses.

    Matching rules:
    - Match on normalized `description`.
    - A refund can only offset expenses with `completed_date` <= refund `completed_date`.
    - Allocation order is most-recent-first among eligible expenses (LIFO).

    This function is intentionally used only for aggregated spend metrics.
    """

    if base.empty:
        return pd.Series(dtype="float")

    exp = base[base.get("type", "").astype(str).str.casefold().eq("expense")].copy()
    if exp.empty:
        return pd.Series(dtype="float")

    exp_amount = pd.to_numeric(exp.get("amount_dkk"), errors="coerce").abs()
    exp = exp.loc[exp_amount.notna()].copy()
    if exp.empty:
        return pd.Series(dtype="float")

    exp["_remaining_spend"] = exp_amount.loc[exp.index].astype(float)
    exp["_desc_norm"] = exp.get("description", pd.Series("", index=exp.index)).astype(str).map(normalize_text)
    exp["_completed"] = pd.to_datetime(exp.get("completed_date"), errors="coerce")

    ref = base[base.get("type", "").astype(str).str.casefold().eq("refund")].copy()
    if ref.empty:
        return exp["_remaining_spend"]

    ref["_refund_amt"] = pd.to_numeric(ref.get("amount_dkk"), errors="coerce").abs()
    ref["_desc_norm"] = ref.get("description", pd.Series("", index=ref.index)).astype(str).map(normalize_text)
    ref["_completed"] = pd.to_datetime(ref.get("completed_date"), errors="coerce")
    ref = ref.loc[
        ref["_refund_amt"].notna()
        & ref["_completed"].notna()
        & ref["_desc_norm"].ne("")
    ].copy()

    if ref.empty:
        return exp["_remaining_spend"]

    ref = ref.sort_values(["_completed"], ascending=[True])

    for _idx, refund in ref.iterrows():
        remaining_refund = float(refund["_refund_amt"])
        if remaining_refund <= 0:
            continue

        eligible = exp[
            exp["_desc_norm"].eq(refund["_desc_norm"])
            & exp["_completed"].notna()
            & exp["_completed"].le(refund["_completed"])
            & exp["_remaining_spend"].gt(0)
        ]
        if eligible.empty:
            continue

        # Most recent eligible expense first.
        eligible = eligible.sort_values(["_completed"], ascending=[False])

        for idx in eligible.index.tolist():
            if remaining_refund <= 0:
                break
            cur = float(exp.at[idx, "_remaining_spend"])
            if cur <= 0:
                continue
            used = min(cur, remaining_refund)
            exp.at[idx, "_remaining_spend"] = cur - used
            remaining_refund -= used

    return exp["_remaining_spend"]


@dataclass(frozen=True)
class PreparedData:
    df: pd.DataFrame
    totals_by_month: pd.DataFrame
    spend_by_month_category: pd.DataFrame
    other_expenses: pd.DataFrame


def prepare_data_for_plotting(csv_path: str, manual_data_dir: str | Path = "data") -> PreparedData:
    """End-to-end prep used by Streamlit plotting."""

    raw = load_revolut_csv(csv_path)
    df = normalize_revolut_df(raw)
    df["type"] = classify_type(df)

    manual = load_manual_expenses(manual_data_dir)
    if not manual.empty:
        df = pd.concat([df, manual], ignore_index=True, sort=False)

    df = categorize_expenses(df)
    df = convert_to_dkk(df)
    success_mask = successful_transaction_mask(df)

    base = df.copy()
    base = base[success_mask].copy()
    base = base[base["completed_date"].notna()].copy() if "completed_date" in base.columns else base
    base["amount_dkk"] = pd.to_numeric(base.get("amount_dkk"), errors="coerce")
    base = base[base["amount_dkk"].notna()].copy()

    if base.empty or "completed_date" not in base.columns:
        totals = pd.DataFrame(columns=["expense", "income", "refund"])
        by_month_cat = pd.DataFrame(columns=["month", "category", "spend_dkk", "item_count"])
    else:
        base["month"] = base["completed_date"].dt.to_period("M").astype(str)
        expense_after_refunds = _expense_spend_after_refunds(base)

        types = ["expense", "income", "refund"]
        totals = (
            base[base["type"].isin(types)]
            .assign(value_dkk=lambda x: x["amount_dkk"].abs())
            .groupby(["month", "type"])["value_dkk"]
            .sum()
            .unstack(fill_value=0.0)
        )

        # Replace gross expense totals with net expense totals after refund allocation.
        if not expense_after_refunds.empty:
            exp_rows = base.loc[expense_after_refunds.index].copy()
            exp_rows["_net_spend"] = expense_after_refunds
            net_exp_by_month = exp_rows.groupby("month")["_net_spend"].sum()
            totals["expense"] = 0.0
            totals.loc[net_exp_by_month.index, "expense"] = net_exp_by_month.values

        for t in types:
            if t not in totals.columns:
                totals[t] = 0.0

        exp = base[base["type"].astype(str).str.casefold().eq("expense")].copy()
        exp = exp[exp.get("category").notna()].copy() if "category" in exp.columns else exp.iloc[0:0]
        if not exp.empty:
            exp = exp.loc[exp.index.intersection(expense_after_refunds.index)].copy()
            exp["_net_spend"] = expense_after_refunds.reindex(exp.index).fillna(0.0)
        by_month_cat = (
            exp.assign(spend_dkk=lambda x: x["_net_spend"])
            .groupby(["month", "category"])["spend_dkk"]
            .agg(spend_dkk="sum", item_count="count")
            .reset_index()
        )

    other_df = df.copy()
    other_df = other_df[success_mask].copy()
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
