"""Jyske Bank statement loader.

Real export (Numbers / Mit Jyske):
- semicolon-separated
- date as DD.MM.YYYY
- amount as -4,021.00 (comma thousands, period decimals)
- columns: Date, Text, Amount, Balance, ..., MainCategory, Category

A durable `*jyske_reference*.csv` keeps history (from 2023). Each new Mit Jyske
export only covers a recent window; it is merged into that reference (overlap
deduped, nothing dropped) and the reference is rewritten through the newest day.

Only a small allow-list is kept for dashboard calculations. Revolut top-ups,
savings moves, unlabeled transfers, and salary are dropped so they are not
double-counted with Revolut.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import logging
import re
from typing import Iterable

import numpy as np
import pandas as pd

from processing import normalize_text, to_snake

logger = logging.getLogger(__name__)

BANK_JYSKE = "jyske"
JYSKE_SAVED_FILENAME = "jyske-statement.csv"
JYSKE_REFERENCE_NAME_PART = "jyske_reference"
TEMPLATE_PATH = Path(__file__).with_name("templates") / "jyske_statement.csv"

COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "completed_date": (
        "date",
        "dato",
        "bogforingsdato",
        "bogføringsdato",
    ),
    "description": (
        "text",
        "tekst",
        "description",
        "beskrivelse",
    ),
    "amount": (
        "amount",
        "beloeb",
        "beløb",
        "sum",
    ),
    "currency": ("valuta", "currency"),
}

# Exact description (normalized) -> dashboard category.
JYSKE_EXPENSE_CATEGORY: dict[str, str] = {
    "akademikernes": "Unions",
    "bs jyske realkredit": "Loan",
    "bs parkeringslauget parkkanten": "Home",
    "bs pure gym denmark a/s": "Health",
    "bs skattestyrelsen motor opkraevning": "Car service",
    "bs skattestyrelsen motor opkrævning": "Car service",
    "ida div.kontingent": "Unions",
    "letsikring af barn ved do": "Insurance",
    "letsikring af barn ved dø": "Insurance",
    "omkostninger, netbank og mobilbank": "Bank fees",
    "to tryg forsikring": "Insurance",
}

JYSKE_REFUND_PREFIXES: tuple[str, ...] = (
    "mobilepay boozt.com",
    "mobilepay boozt",
)

DEFAULT_SEARCH_DIRS = (
    "/Users/mehdiordikhani/Library/Mobile Documents/com~apple~Numbers/Documents",
    "data",
)

_SKIP_NAME_PARTS = (
    "account-statement",
    "savings-statement",
    "consolidated_statement",
    "manual_expenses",
    "template",
)

# Mit Jyske / Numbers export: "Mehdi Ordikhani_2026-01-01-2026-08-24.csv"
_JYSKE_NAME_DATE_RANGE = re.compile(
    r"_\d{4}-\d{2}-\d{2}-\d{4}-\d{2}-\d{2}\.csv$",
    re.IGNORECASE,
)
_CSV_DATES = re.compile(r"\d{4}-\d{2}-\d{2}")


def jyske_template_path() -> Path:
    return TEMPLATE_PATH


def saved_jyske_csv_path(data_dir: str | Path = "data") -> Path:
    return Path(data_dir) / JYSKE_SAVED_FILENAME


def default_jyske_reference_path(data_dir: str | Path = "data") -> Path:
    return Path(data_dir) / f"{JYSKE_REFERENCE_NAME_PART}.csv"


@dataclass
class JyskeMergeResult:
    path: str
    row_count: int = 0
    min_date: str | None = None
    max_date: str | None = None
    added_rows: int = 0
    merged_files: tuple[str, ...] = field(default_factory=tuple)
    gap_warning: str | None = None


def _header_looks_like_jyske(path: Path) -> bool:
    try:
        header = path.read_text(encoding="utf-8-sig", errors="replace").splitlines()[0]
    except Exception:
        return False
    sep = ";" if header.count(";") >= header.count(",") else ","
    cols = {to_snake(c.strip()) for c in header.strip().split(sep)}
    return {"date", "text", "amount"}.issubset(cols) and "accountname" in cols


def _is_jyske_reference_path(path: Path) -> bool:
    return JYSKE_REFERENCE_NAME_PART in path.name.casefold()


def _is_jyske_export(path: Path) -> bool:
    """True for Jyske checking-account exports, never Revolut/savings/manual files."""
    name = path.name.casefold()
    if any(part in name for part in _SKIP_NAME_PARTS):
        return False
    if "jyske" in name:
        return True
    if _JYSKE_NAME_DATE_RANGE.search(path.name):
        return _header_looks_like_jyske(path)
    return _header_looks_like_jyske(path)


def _jyske_sort_key(p: Path):
    dates = [pd.to_datetime(d, errors="coerce") for d in _CSV_DATES.findall(p.name)]
    dates = [d for d in dates if pd.notna(d)]
    end = dates[-1] if dates else pd.Timestamp("1900-01-01")
    start = dates[0] if dates else pd.Timestamp("1900-01-01")
    return (end, start, p.stat().st_mtime)


def _iter_jyske_candidates(search_dirs: Iterable[str | Path]) -> list[Path]:
    candidates: list[Path] = []
    for folder in search_dirs:
        base = Path(folder)
        if not base.exists() or not base.is_dir():
            continue
        for p in base.glob("*.csv"):
            if _is_jyske_export(p):
                candidates.append(p)
    return candidates


def find_latest_jyske_statement_csv(
    search_dirs: Iterable[str | Path] | None = None,
) -> str | None:
    """Return the newest Jyske CSV path, or None if none has been uploaded yet.

    Recency uses the last YYYY-MM-DD in the filename, then modified time.
    """

    dirs = list(search_dirs if search_dirs is not None else DEFAULT_SEARCH_DIRS)
    candidates = _iter_jyske_candidates(dirs)
    if not candidates:
        return None
    best = sorted(candidates, key=_jyske_sort_key, reverse=True)[0]
    return str(best.as_posix())


def cleanup_outdated_jyske_statement_csvs(
    search_dirs: Iterable[str | Path] | None = None,
    keep_path: str | None = None,
) -> list[str]:
    """Delete older incremental Jyske exports, keeping `keep_path`.

    Never deletes the durable `*jyske_reference*` file, nor Revolut / savings /
    consolidated / manual_expenses files.
    """

    if not keep_path:
        return []

    dirs = list(search_dirs if search_dirs is not None else DEFAULT_SEARCH_DIRS)
    keep_resolved = None
    if keep_path:
        try:
            keep_resolved = Path(keep_path).resolve()
        except Exception:
            keep_resolved = None

    deleted: list[str] = []
    for p in _iter_jyske_candidates(dirs):
        if _is_jyske_reference_path(p):
            continue
        if keep_resolved is not None:
            try:
                if p.resolve() == keep_resolved:
                    continue
            except Exception:
                if str(p.as_posix()) == str(keep_path):
                    continue
        elif keep_path and str(p.as_posix()) == str(keep_path):
            continue
        try:
            p.unlink()
            deleted.append(str(p.as_posix()))
        except Exception:
            continue
    return deleted


def find_jyske_reference_csv(
    search_dirs: Iterable[str | Path] | None = None,
) -> Path | None:
    """Return the durable Jyske reference path, if one exists."""
    dirs = list(search_dirs if search_dirs is not None else DEFAULT_SEARCH_DIRS)
    named = [p for p in _iter_jyske_candidates(dirs) if _is_jyske_reference_path(p)]
    if named:
        return max(named, key=lambda p: p.stat().st_size)

    saved = saved_jyske_csv_path("data")
    if saved.exists() and saved.stat().st_size > 0 and _header_looks_like_jyske(saved):
        return saved
    return None


def _iter_jyske_incrementals(
    search_dirs: Iterable[str | Path],
    reference: Path | None,
) -> list[Path]:
    ref_resolved = None
    if reference is not None:
        try:
            ref_resolved = reference.resolve()
        except Exception:
            ref_resolved = None

    out: list[Path] = []
    for p in _iter_jyske_candidates(search_dirs):
        if _is_jyske_reference_path(p):
            continue
        if ref_resolved is not None:
            try:
                if p.resolve() == ref_resolved:
                    continue
            except Exception:
                pass
        out.append(p)
    return sorted(out, key=_jyske_sort_key)


def _read_jyske_raw(path: Path) -> pd.DataFrame:
    text = path.read_text(encoding="utf-8-sig", errors="replace")
    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]
    if not lines:
        return pd.DataFrame()
    sep = _detect_separator(lines[0])
    raw = pd.read_csv(path, sep=sep, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    if raw.empty:
        return pd.DataFrame()
    raw.columns = [str(c).strip() for c in raw.columns]
    return raw


def _jyske_date_series(df: pd.DataFrame) -> pd.Series:
    date_col = _resolve_column(df, COLUMN_ALIASES["completed_date"])
    if not date_col:
        return pd.Series(pd.NaT, index=df.index)
    return pd.to_datetime(df[date_col], dayfirst=True, errors="coerce")


def _jyske_dedup_key(df: pd.DataFrame) -> pd.Series:
    """Stable identity for a Jyske row across export formatting differences."""
    date_col = _resolve_column(df, COLUMN_ALIASES["completed_date"])
    desc_col = _resolve_column(df, COLUMN_ALIASES["description"])
    amt_col = _resolve_column(df, COLUMN_ALIASES["amount"])
    bal_col = _resolve_column(df, ("balance", "saldo"))

    if date_col:
        dates = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce").dt.strftime("%Y-%m-%d")
        dates = dates.where(dates.notna(), "")
    else:
        dates = pd.Series("", index=df.index)

    if desc_col:
        desc = df[desc_col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    else:
        desc = pd.Series("", index=df.index)

    if amt_col:
        amt = df[amt_col].map(parse_jyske_amount).map(lambda x: "" if pd.isna(x) else f"{float(x):.2f}")
    else:
        amt = pd.Series("", index=df.index)

    if bal_col:
        bal = df[bal_col].map(parse_jyske_amount).map(lambda x: "" if pd.isna(x) else f"{float(x):.2f}")
    else:
        bal = pd.Series("", index=df.index)

    return dates + "|" + desc + "|" + amt + "|" + bal


def _jyske_date_span(df: pd.DataFrame) -> tuple[str | None, str | None]:
    dates = _jyske_date_series(df).dropna()
    if dates.empty:
        return None, None
    return str(dates.min().date()), str(dates.max().date())


def merge_jyske_raw(
    reference: pd.DataFrame,
    incoming: pd.DataFrame,
) -> tuple[pd.DataFrame, int, str | None]:
    """Union two raw Jyske exports.

    Incoming rows win on exact duplicates so a later export can correct an amount.
    History that exists only in the reference is kept. Returns
    (merged, added_row_count, gap_warning).
    """
    if incoming.empty:
        return reference.copy() if not reference.empty else incoming.copy(), 0, None
    if reference.empty:
        return incoming.copy(), int(len(incoming)), None

    ref_dates = _jyske_date_series(reference)
    new_dates = _jyske_date_series(incoming)
    ref_max = ref_dates.max()
    new_min = new_dates.min()
    gap_warning = None
    if pd.notna(ref_max) and pd.notna(new_min) and new_min > ref_max + pd.Timedelta(days=1):
        gap_warning = (
            f"Jyske reference ends {ref_max.date()} but the new export starts "
            f"{new_min.date()}. History in between is not in either file."
        )
        logger.warning(gap_warning)

    cols: list[str] = list(reference.columns)
    for c in incoming.columns:
        if c not in cols:
            cols.append(c)

    incoming_aligned = incoming.reindex(columns=cols)
    reference_aligned = reference.reindex(columns=cols)
    combined = pd.concat([incoming_aligned, reference_aligned], ignore_index=True)
    combined["_key"] = _jyske_dedup_key(combined)
    combined["_ord"] = range(len(combined))
    before = len(reference)
    combined = combined.drop_duplicates(subset="_key", keep="first")
    combined["_date"] = _jyske_date_series(combined)
    combined = combined.sort_values(["_date", "_ord"], ascending=[False, True], kind="stable")
    combined = combined.drop(columns=["_key", "_ord", "_date"]).reset_index(drop=True)
    added = max(0, len(combined) - before)
    return combined, added, gap_warning


def _write_jyske_raw(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.to_csv(tmp, sep=";", index=False, encoding="utf-8-sig", lineterminator="\n")
    tmp.replace(path)


def ensure_jyske_reference_merged(
    search_dirs: Iterable[str | Path] | None = None,
    data_dir: str | Path = "data",
) -> JyskeMergeResult | None:
    """Merge new Jyske exports into the durable reference and return its path.

    The reference file is never deleted. After a successful merge it is rewritten
    so its newest row matches the newest day of the latest upload.
    """
    dirs = list(search_dirs if search_dirs is not None else DEFAULT_SEARCH_DIRS)
    reference = find_jyske_reference_csv(dirs)
    incrementals = _iter_jyske_incrementals(dirs, reference)

    if reference is None:
        if not incrementals:
            return None
        reference = default_jyske_reference_path(data_dir)
        merged = _read_jyske_raw(incrementals[0])
        added_total = 0
        merged_names = [incrementals[0].name]
        warnings: list[str] = []
        for inc in incrementals[1:]:
            incoming = _read_jyske_raw(inc)
            merged, added, warn = merge_jyske_raw(merged, incoming)
            added_total += added
            if added:
                merged_names.append(inc.name)
            if warn:
                warnings.append(warn)
        if not merged.empty:
            _write_jyske_raw(reference, merged)
        min_date, max_date = _jyske_date_span(merged)
        return JyskeMergeResult(
            path=str(reference.as_posix()),
            row_count=int(len(merged)),
            min_date=min_date,
            max_date=max_date,
            added_rows=added_total,
            merged_files=tuple(merged_names),
            gap_warning="; ".join(warnings) if warnings else None,
        )

    merged = _read_jyske_raw(reference)
    added_total = 0
    merged_names: list[str] = []
    warnings: list[str] = []
    for inc in incrementals:
        incoming = _read_jyske_raw(inc)
        if incoming.empty:
            continue
        merged, added, warn = merge_jyske_raw(merged, incoming)
        added_total += added
        if added:
            merged_names.append(inc.name)
        if warn:
            warnings.append(warn)

    if added_total:
        _write_jyske_raw(reference, merged)
        logger.info(
            "Updated Jyske reference %s with +%s rows from %s",
            reference,
            added_total,
            ", ".join(merged_names),
        )

    min_date, max_date = _jyske_date_span(merged)
    return JyskeMergeResult(
        path=str(reference.as_posix()),
        row_count=int(len(merged)),
        min_date=min_date,
        max_date=max_date,
        added_rows=added_total,
        merged_files=tuple(merged_names),
        gap_warning="; ".join(warnings) if warnings else None,
    )


def _detect_separator(header: str) -> str:
    return ";" if header.count(";") >= header.count(",") else ","


def parse_jyske_amount(value: object) -> float:
    """Parse Jyske amounts: '-4,021.00' (this export) or Danish '-4.021,00'."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return float("nan")
    if isinstance(value, (int, float, np.integer, np.floating)) and not isinstance(value, bool):
        return float(value)

    s = str(value).strip().replace("\xa0", "").replace(" ", "")
    if not s or s in {"-", "–", "−"}:
        return float("nan")

    negative = s.startswith(("-", "−")) or s.endswith("-")
    s = s.strip("-").strip("−")
    if "," in s and "." in s:
        if s.rfind(".") > s.rfind(","):
            s = s.replace(",", "")
        else:
            s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        left, _, right = s.partition(",")
        s = f"{left}.{right}" if len(right) == 2 else s.replace(",", "")
    try:
        n = float(s)
    except ValueError:
        return float("nan")
    return -abs(n) if negative else n


def _resolve_column(df: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    snake_cols = {to_snake(c): c for c in df.columns}
    for alias in aliases:
        key = to_snake(alias)
        if key in snake_cols:
            return snake_cols[key]
    return None


def _norm_key(text: object) -> str:
    t = normalize_text(text)
    t = t.replace("ø", "o").replace("æ", "ae").replace("å", "a")
    return t


def _expense_category_for(description: str) -> str | None:
    key = _norm_key(description)
    if key in JYSKE_EXPENSE_CATEGORY:
        return JYSKE_EXPENSE_CATEGORY[key]
    for needle, category in JYSKE_EXPENSE_CATEGORY.items():
        if key == _norm_key(needle) or key.startswith(_norm_key(needle)):
            return category
    return None


def _is_jyske_refund(description: str) -> bool:
    key = _norm_key(description)
    return any(key.startswith(prefix) for prefix in JYSKE_REFUND_PREFIXES)


def classify_included_jyske(frame: pd.DataFrame) -> pd.DataFrame:
    """Keep only allow-listed rows and assign type + category."""
    if frame.empty:
        return frame

    out = frame.copy()
    desc = out["description"].astype(str)
    cats = desc.map(_expense_category_for)
    is_refund = desc.map(_is_jyske_refund)
    keep = cats.notna() | is_refund
    out = out.loc[keep].copy()
    if out.empty:
        return out

    desc = out["description"].astype(str)
    cats = desc.map(_expense_category_for)
    is_refund = desc.map(_is_jyske_refund)
    out["type"] = np.where(is_refund, "refund", "expense")
    out["category"] = np.where(is_refund, pd.NA, cats)
    return out.reset_index(drop=True)


def load_jyske_statement(csv_path: str | Path | None) -> pd.DataFrame:
    """Load a Jyske CSV into the dashboard schema. Empty if path is missing."""
    if not csv_path:
        return pd.DataFrame()

    p = Path(csv_path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()

    text = p.read_text(encoding="utf-8-sig", errors="replace")
    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]
    if not lines:
        return pd.DataFrame()

    sep = _detect_separator(lines[0])
    raw = pd.read_csv(p, sep=sep, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    if raw.empty:
        return pd.DataFrame()

    raw.columns = [str(c).strip() for c in raw.columns]
    date_col = _resolve_column(raw, COLUMN_ALIASES["completed_date"])
    desc_col = _resolve_column(raw, COLUMN_ALIASES["description"])
    amt_col = _resolve_column(raw, COLUMN_ALIASES["amount"])
    if not date_col or not desc_col or not amt_col:
        return pd.DataFrame()

    out = pd.DataFrame()
    out["completed_date"] = pd.to_datetime(raw[date_col], dayfirst=True, errors="coerce")
    out["description"] = raw[desc_col].astype(str).str.strip()
    out["amount_net"] = raw[amt_col].map(parse_jyske_amount)

    ccy_col = _resolve_column(raw, COLUMN_ALIASES["currency"])
    if ccy_col:
        out["currency"] = raw[ccy_col].astype(str).str.upper().str.strip().replace({"": "DKK"})
    else:
        out["currency"] = "DKK"

    out["fee"] = 0.0
    out["sub_type"] = "Jyske"
    out["source"] = BANK_JYSKE
    out["bank"] = BANK_JYSKE
    out = out[out["completed_date"].notna() & out["amount_net"].notna() & out["description"].ne("")].copy()
    out = classify_included_jyske(out)
    return out.reset_index(drop=True)
