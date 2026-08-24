"""Jyske Bank statement loader.

Jyske exports a different layout than Revolut. This module maps a Jyske CSV onto
the internal schema used by the dashboard (completed_date, description,
amount_net, currency, type, bank).

When a real export is uploaded, adjust:
- COLUMN_ALIASES (header names)
- _detect_separator / _parse_dk_amount (delimiter and number format)
- classify_jyske_type (expense vs income vs refund)
- date parsing (dayfirst)
"""

from __future__ import annotations

from pathlib import Path
import re
from typing import Iterable

import numpy as np
import pandas as pd

from processing import to_snake

BANK_JYSKE = "jyske"
JYSKE_FILENAME_TOKEN = "jyske"
JYSKE_SAVED_FILENAME = "jyske-statement.csv"
TEMPLATE_PATH = Path(__file__).with_name("templates") / "jyske_statement.csv"

# Placeholder aliases for a typical Danish netbank CSV (semicolon, comma decimals).
# Extend this map when the real Jyske headers are known.
COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "completed_date": (
        "dato",
        "bogforingsdato",
        "bogføringsdato",
        "valørdato",
        "valoerdato",
        "date",
    ),
    "description": (
        "tekst",
        "tekstforklaring",
        "beskrivelse",
        "description",
        "text",
    ),
    "amount": (
        "beloeb",
        "beløb",
        "amount",
        "sum",
    ),
    "balance": ("saldo", "balance"),
    "currency": ("valuta", "currency", "mont", "mønt"),
}

DEFAULT_SEARCH_DIRS = (
    "/Users/mehdiordikhani/Library/Mobile Documents/com~apple~Numbers/Documents",
    "data",
)


def jyske_template_path() -> Path:
    return TEMPLATE_PATH


def saved_jyske_csv_path(data_dir: str | Path = "data") -> Path:
    return Path(data_dir) / JYSKE_SAVED_FILENAME


def find_latest_jyske_statement_csv(
    search_dirs: Iterable[str | Path] | None = None,
) -> str | None:
    """Return the newest Jyske CSV path, or None if none has been uploaded yet.

    Looks for filenames containing 'jyske' (case-insensitive), ignoring templates.
    """

    dirs = [Path(d) for d in (search_dirs if search_dirs is not None else DEFAULT_SEARCH_DIRS)]
    candidates: list[Path] = []
    token = JYSKE_FILENAME_TOKEN.casefold()
    for folder in dirs:
        if not folder.exists() or not folder.is_dir():
            continue
        for p in folder.glob("*.csv"):
            name = p.name.casefold()
            if token not in name:
                continue
            if "template" in name:
                continue
            candidates.append(p)

    if not candidates:
        return None

    date_re = re.compile(r"\d{4}-\d{2}-\d{2}")

    def sort_key(p: Path):
        dates = [pd.to_datetime(d, errors="coerce") for d in date_re.findall(p.name)]
        dates = [d for d in dates if pd.notna(d)]
        end = dates[-1] if dates else pd.Timestamp("1900-01-01")
        return (end, p.stat().st_mtime)

    best = sorted(candidates, key=sort_key, reverse=True)[0]
    return str(best.as_posix())


def _detect_separator(header: str) -> str:
    return ";" if header.count(";") >= header.count(",") else ","


def _parse_dk_amount(value: object) -> float:
    """Parse Danish-style amounts like '-1.234,56' or '1234,56-'. Adjust if needed."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return float("nan")
    if isinstance(value, (int, float, np.integer, np.floating)) and not isinstance(value, bool):
        return float(value)

    s = str(value).strip().replace("\xa0", "").replace(" ", "")
    if not s or s in {"-", "–", "−"}:
        return float("nan")

    negative = s.startswith("-") or s.startswith("−") or s.endswith("-")
    s = s.strip("-").strip("−")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
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


def classify_jyske_type(frame: pd.DataFrame) -> pd.Series:
    """Placeholder type rules. Revisit when the real Jyske export is in hand.

    Current assumption: negative amount_net = expense, positive = income.
    Descriptions containing refund/tilbagefør are marked refund.
    """

    amt = pd.to_numeric(frame.get("amount_net"), errors="coerce")
    out = pd.Series(pd.NA, index=frame.index, dtype="object")
    out.loc[amt < 0] = "expense"
    out.loc[amt > 0] = "income"
    desc = frame.get("description", pd.Series("", index=frame.index)).astype(str)
    is_refund = desc.str.contains(r"refund|tilbagefør|tilbagefor", case=False, na=False)
    out.loc[is_refund] = "refund"
    return out


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
    out["amount_net"] = raw[amt_col].map(_parse_dk_amount)

    ccy_col = _resolve_column(raw, COLUMN_ALIASES["currency"])
    if ccy_col:
        out["currency"] = raw[ccy_col].astype(str).str.upper().str.strip().replace({"": "DKK"})
    else:
        out["currency"] = "DKK"

    out["fee"] = 0.0
    out["sub_type"] = "Jyske"
    out["source"] = BANK_JYSKE
    out["bank"] = BANK_JYSKE
    out["type"] = classify_jyske_type(out)

    out = out[out["completed_date"].notna() & out["amount_net"].notna() & out["description"].ne("")].copy()
    return out.reset_index(drop=True)
