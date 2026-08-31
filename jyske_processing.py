"""Jyske Bank statement loader.

Real export (Numbers / Mit Jyske):
- semicolon-separated
- date as DD.MM.YYYY
- amount as -4,021.00 (comma thousands, period decimals)
- columns: Date, Text, Amount, Balance, ..., MainCategory, Category

A durable `*jyske_reference*.csv` keeps history (from 2023). Each new Mit Jyske
export (CSV or PDF) only covers a recent window; the newest file is merged into
that reference (overlap deduped, nothing dropped) and the reference is rewritten
through the newest day. The reference itself stays a CSV and is never deleted.
Processed incrementals are then removed. Karoline uses the same flow in
`faimly_files` with `karoline_jyske_reference.csv` (Annual tab only).

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

from processing import CATEGORY_PERSONAL, PERSON_KAROLINE, PERSON_MEHDI, normalize_text, to_snake

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

# Karoline: case-insensitive contains (ø/Ø folded). First match wins.
KAROLINE_JYSKE_CONTAINS: tuple[tuple[str, str], ...] = (
    ("rødovre kommune", "Home tax"),
    ("sankt petri skole", "School"),
    ("ejerforeningen parkkanten", "Apartments"),
    ("andel energi", "Energy"),
    ("holdsport", "Ice Hockey"),
    ("skatertown", "Ice Hockey"),
    ("skoejte", "Ice Hockey"),
    ("skøjte", "Ice Hockey"),
    ("rsik", "Ice Hockey"),
    ("mighty bulls", "Ice Hockey"),
    ("max hockey", "Ice Hockey"),
    ("hockeyshop", "Ice Hockey"),
    ("hockeystore", "Ice Hockey"),
    ("rexhockey", "Ice Hockey"),
    ("bauer hockey", "Ice Hockey"),
    ("ishockey", "Ice Hockey"),
    ("copenhagen fal", "Ice Hockey"),
    ("falcons camps", "Ice Hockey"),
    ("serc04", "Ice Hockey"),
)
KAROLINE_JYSKE_CATEGORIES: frozenset[str] = frozenset(cat for _needle, cat in KAROLINE_JYSKE_CONTAINS)

DEFAULT_SEARCH_DIRS = (
    "/Users/mehdiordikhani/Library/Mobile Documents/com~apple~Numbers/Documents",
    "data",
)
KAROLINE_SEARCH_DIRS = (
    "/Users/mehdiordikhani/Library/Mobile Documents/com~apple~CloudDocs/faimly_files",
)
KAROLINE_REFERENCE_NAME_PART = "karoline_jyske_reference"

_SKIP_NAME_PARTS = (
    "account-statement",
    "savings-statement",
    "consolidated_statement",
    "manual_expenses",
    "template",
)

# Mit Jyske / Numbers export: "Mehdi Ordikhani_2026-01-01-2026-08-24.csv"
_JYSKE_NAME_DATE_RANGE = re.compile(
    r"_\d{4}-\d{2}-\d{2}-\d{4}-\d{2}-\d{2}\.(csv|pdf)$",
    re.IGNORECASE,
)
_CSV_DATES = re.compile(r"\d{4}-\d{2}-\d{2}")
_PDF_AMOUNT = r"-?(?:\d{1,3}(?:,\d{3})+|\d+)\.\d{2}"
_PDF_ROW_RE = re.compile(
    rf"^(?P<date>\d{{2}}\.\d{{2}}\.\d{{4}})\s+"
    rf"(?P<text>.+?)\s+"
    rf"(?P<amount>{_PDF_AMOUNT})\s+"
    rf"(?P<balance>{_PDF_AMOUNT})"
    rf"(?:\s+\S+)?\s*$"
)
_PDF_DK_AMOUNT = r"-?(?:\d{1,3}(?:\.\d{3})+|\d+),\d{2}"
_PDF_DK_ROW_RE = re.compile(
    rf"^(?P<date>\d{{2}}\.\d{{2}}\.\d{{4}})\s+"
    rf"(?P<text>.+?)\s+"
    rf"(?P<amount>{_PDF_DK_AMOUNT})\s+"
    rf"(?P<balance>{_PDF_DK_AMOUNT})\s*$"
)


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
    removed_files: tuple[str, ...] = field(default_factory=tuple)
    gap_warning: str | None = None


def _header_looks_like_jyske(path: Path) -> bool:
    try:
        header = path.read_text(encoding="utf-8-sig", errors="replace").splitlines()[0]
    except Exception:
        return False
    sep = ";" if header.count(";") >= header.count(",") else ","
    cols = {to_snake(c.strip()) for c in header.strip().split(sep)}
    return {"date", "text", "amount"}.issubset(cols) and "accountname" in cols


def _is_jyske_reference_path(
    path: Path,
    reference_name_part: str = JYSKE_REFERENCE_NAME_PART,
) -> bool:
    name = path.name.casefold()
    part = reference_name_part.casefold()
    if part == JYSKE_REFERENCE_NAME_PART:
        return JYSKE_REFERENCE_NAME_PART in name and KAROLINE_REFERENCE_NAME_PART not in name
    return part in name


def _extract_pdf_text(path: Path) -> str:
    from pypdf import PdfReader

    reader = PdfReader(str(path))
    return "\n".join((page.extract_text() or "") for page in reader.pages)


def _pdf_looks_like_jyske(path: Path) -> bool:
    try:
        head = _extract_pdf_text(path)[:2000]
    except Exception:
        return False
    return (
        "Account Entries" in head
        or "Chosen account" in head
        or "Kontobevægelser" in head
        or "Valgte konti" in head
        or ("Date" in head and "Amount" in head and "Balance" in head)
        or ("Dato" in head and "Beløb" in head and "Saldo" in head)
    )


def _is_jyske_export(path: Path, *, any_jyske_file: bool = False) -> bool:
    """True for Jyske checking-account exports, never Revolut/savings/manual files."""
    name = path.name.casefold()
    if any(part in name for part in _SKIP_NAME_PARTS):
        return False
    if path.suffix.lower() == ".pdf":
        if any_jyske_file or "jyske" in name or _JYSKE_NAME_DATE_RANGE.search(path.name):
            return _pdf_looks_like_jyske(path)
        return False
    if any_jyske_file:
        return _header_looks_like_jyske(path)
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


def _keep_latest_export_file(
    paths: list[Path],
    reference_name_part: str = JYSKE_REFERENCE_NAME_PART,
) -> list[Path]:
    """If CSV and PDF share a stem (same export window), keep the newest mtime."""
    best: dict[str, Path] = {}
    for p in paths:
        if _is_jyske_reference_path(p, reference_name_part):
            key = f"ref::{p.name.casefold()}"
        else:
            key = p.stem.casefold()
        prev = best.get(key)
        if prev is None or p.stat().st_mtime >= prev.stat().st_mtime:
            best[key] = p
    return list(best.values())


def _iter_jyske_files(
    search_dirs: Iterable[str | Path],
    *,
    any_jyske_file: bool = False,
) -> list[Path]:
    candidates: list[Path] = []
    for folder in search_dirs:
        base = Path(folder)
        if not base.exists() or not base.is_dir():
            continue
        for p in (*base.glob("*.csv"), *base.glob("*.pdf")):
            if _is_jyske_export(p, any_jyske_file=any_jyske_file):
                candidates.append(p)
    return candidates


def _iter_jyske_candidates(
    search_dirs: Iterable[str | Path],
    *,
    any_jyske_file: bool = False,
    reference_name_part: str = JYSKE_REFERENCE_NAME_PART,
) -> list[Path]:
    return _keep_latest_export_file(
        _iter_jyske_files(search_dirs, any_jyske_file=any_jyske_file),
        reference_name_part=reference_name_part,
    )


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
    *,
    reference_name_part: str = JYSKE_REFERENCE_NAME_PART,
    any_jyske_file: bool = False,
    allow_saved_fallback: bool = True,
) -> Path | None:
    """Return the durable Jyske reference path, if one exists."""
    dirs = list(search_dirs if search_dirs is not None else DEFAULT_SEARCH_DIRS)
    named = [
        p
        for p in _iter_jyske_candidates(
            dirs, any_jyske_file=any_jyske_file, reference_name_part=reference_name_part
        )
        if _is_jyske_reference_path(p, reference_name_part)
    ]
    if named:
        return max(named, key=lambda p: p.stat().st_size)

    if allow_saved_fallback:
        saved = saved_jyske_csv_path("data")
        if saved.exists() and saved.stat().st_size > 0 and _header_looks_like_jyske(saved):
            return saved
    return None


def _iter_jyske_incrementals(
    search_dirs: Iterable[str | Path],
    reference: Path | None,
    *,
    any_jyske_file: bool = False,
    reference_name_part: str = JYSKE_REFERENCE_NAME_PART,
) -> list[Path]:
    ref_resolved = None
    if reference is not None:
        try:
            ref_resolved = reference.resolve()
        except Exception:
            ref_resolved = None

    out: list[Path] = []
    for p in _iter_jyske_candidates(
        search_dirs, any_jyske_file=any_jyske_file, reference_name_part=reference_name_part
    ):
        if _is_jyske_reference_path(p, reference_name_part):
            continue
        if ref_resolved is not None:
            try:
                if p.resolve() == ref_resolved:
                    continue
            except Exception:
                pass
        out.append(p)
    return sorted(out, key=_jyske_sort_key)


def _delete_processed_incrementals(
    incrementals: Iterable[Path],
    reference: Path | None,
    reference_name_part: str = JYSKE_REFERENCE_NAME_PART,
) -> list[str]:
    """Delete processed uploads (and CSV/PDF twins). Never deletes the reference."""
    ref_resolved = None
    if reference is not None:
        try:
            ref_resolved = reference.resolve()
        except Exception:
            ref_resolved = None

    to_delete: set[Path] = set()
    for p in incrementals:
        to_delete.add(p)
        for suffix in (".csv", ".pdf"):
            twin = p.with_suffix(suffix)
            if twin.exists():
                to_delete.add(twin)

    deleted: list[str] = []
    for p in to_delete:
        if _is_jyske_reference_path(p, reference_name_part):
            continue
        if ref_resolved is not None:
            try:
                if p.resolve() == ref_resolved:
                    continue
            except Exception:
                pass
        try:
            p.unlink()
            deleted.append(str(p.as_posix()))
        except Exception:
            continue
    return deleted


def _read_jyske_pdf(path: Path) -> pd.DataFrame:
    try:
        text = _extract_pdf_text(path)
    except Exception as e:
        logger.warning("Failed to read Jyske PDF %s: %s", path, e)
        return pd.DataFrame()

    rows: list[dict[str, str]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.lower().startswith(("page ", "side ", "dokument")):
            continue
        m = _PDF_ROW_RE.match(line) or _PDF_DK_ROW_RE.match(line)
        if not m:
            continue
        rows.append(
            {
                "Date": m.group("date"),
                "Text": m.group("text").strip(),
                "Amount": m.group("amount"),
                "Balance": m.group("balance"),
                "Reconciled": "",
                "AccountNumber": "",
                "AccountName": "",
                "MainCategory": "",
                "Category": "",
                "Comment": "",
            }
        )
    return pd.DataFrame(rows)


def _read_jyske_raw(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".pdf":
        return _read_jyske_pdf(path)

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
    *,
    reference_name_part: str = JYSKE_REFERENCE_NAME_PART,
    default_reference_path: Path | None = None,
    any_jyske_file: bool = False,
    allow_saved_fallback: bool = True,
) -> JyskeMergeResult | None:
    """Merge new Jyske exports into the durable reference and return its path.

    The reference file is never deleted. After a successful merge it is rewritten
    so its newest row matches the newest day of the latest upload, then the
    processed incremental file(s) are removed.
    """
    dirs = list(search_dirs if search_dirs is not None else DEFAULT_SEARCH_DIRS)
    reference = find_jyske_reference_csv(
        dirs,
        reference_name_part=reference_name_part,
        any_jyske_file=any_jyske_file,
        allow_saved_fallback=allow_saved_fallback,
    )
    incrementals = _iter_jyske_incrementals(
        dirs,
        reference,
        any_jyske_file=any_jyske_file,
        reference_name_part=reference_name_part,
    )

    processed: list[Path] = []
    added_total = 0
    merged_names: list[str] = []
    warnings: list[str] = []

    if reference is None:
        if not incrementals:
            return None
        reference = default_reference_path or default_jyske_reference_path(data_dir)
        merged = pd.DataFrame()
        for inc in incrementals:
            incoming = _read_jyske_raw(inc)
            if incoming.empty:
                continue
            merged, added, warn = merge_jyske_raw(merged, incoming)
            added_total += added
            processed.append(inc)
            merged_names.append(inc.name)
            if warn:
                warnings.append(warn)
        if merged.empty:
            return None
        _write_jyske_raw(reference, merged)
    else:
        merged = _read_jyske_raw(reference)
        for inc in incrementals:
            incoming = _read_jyske_raw(inc)
            if incoming.empty:
                continue
            merged, added, warn = merge_jyske_raw(merged, incoming)
            added_total += added
            processed.append(inc)
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

    removed = _delete_processed_incrementals(processed, reference, reference_name_part)
    min_date, max_date = _jyske_date_span(merged)
    return JyskeMergeResult(
        path=str(reference.as_posix()),
        row_count=int(len(merged)),
        min_date=min_date,
        max_date=max_date,
        added_rows=added_total,
        merged_files=tuple(merged_names),
        removed_files=tuple(removed),
        gap_warning="; ".join(warnings) if warnings else None,
    )


def ensure_karoline_jyske_reference_merged() -> JyskeMergeResult | None:
    family_dir = Path(KAROLINE_SEARCH_DIRS[0])
    family_dir.mkdir(parents=True, exist_ok=True)
    return ensure_jyske_reference_merged(
        search_dirs=KAROLINE_SEARCH_DIRS,
        data_dir=family_dir,
        reference_name_part=KAROLINE_REFERENCE_NAME_PART,
        default_reference_path=family_dir / f"{KAROLINE_REFERENCE_NAME_PART}.csv",
        any_jyske_file=True,
        allow_saved_fallback=False,
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


def _karoline_contains(key: str, needle: str) -> bool:
    """Contains-match. Single tokens use word edges so `rsik` ≠ `forsikring`."""
    n = _norm_key(needle)
    if not n:
        return False
    if " " in n:
        return n in key
    return re.search(rf"(?<![a-z0-9]){re.escape(n)}(?![a-z0-9])", key) is not None


def _expense_category_for(description: str, person: str = PERSON_MEHDI) -> str | None:
    key = _norm_key(description)
    if str(person).casefold().strip() == PERSON_KAROLINE:
        for needle, category in KAROLINE_JYSKE_CONTAINS:
            if _karoline_contains(key, needle):
                return category
        return None
    if key in JYSKE_EXPENSE_CATEGORY:
        return JYSKE_EXPENSE_CATEGORY[key]
    for needle, category in JYSKE_EXPENSE_CATEGORY.items():
        if key == _norm_key(needle) or key.startswith(_norm_key(needle)):
            return category
    return None


def _is_jyske_refund(description: str) -> bool:
    key = _norm_key(description)
    return any(key.startswith(prefix) for prefix in JYSKE_REFUND_PREFIXES)


def _is_jyske_non_spend(description: str) -> bool:
    """Salary, savings, Revolut/Wise/Lunar top-ups, and internal transfers."""
    t = str(description).casefold().strip()
    if any(
        x in t
        for x in (
            "revolut",
            "opsparing",
            "oskar",
            "rentegaranti",
            "nordnet",
            "lånesagskonto",
            "wise",
            "lunar",
            "karoline",
            "saving account",
            "internal transfer",
            "children money",
            "oen account",
        )
    ):
        return True
    if t.startswith("lønoverførsel") or "børne- og ungeydelse" in t or "feriepenge" in t or "overskydende skat" in t:
        return True
    if t.startswith("doser, maritta") or "universitetet i oslo" in t:
        return True
    if t in {"overførsel", "mehdi ordikhani", "from lunar", "karoline doser", "myself"}:
        return True
    if t.startswith("overførsel ") or t.startswith("to lara") or t.startswith("to leo") or t.startswith("from karo"):
        return True
    if t.startswith("returned") or t.startswith("rerurning"):
        return True
    if "mehdi" in t:
        return True
    compact = t.replace(" ", "")
    if compact.startswith(("5030", "6695")) or t.startswith("til 6695") or t.startswith("salary august"):
        return True
    return False


def classify_included_jyske(frame: pd.DataFrame) -> pd.DataFrame:
    """Keep allow-listed rows; leftover Jyske outflows become Personal."""
    if frame.empty:
        return frame

    out = frame.copy()
    if "person" not in out.columns:
        out["person"] = PERSON_MEHDI
    desc = out["description"].astype(str)
    persons = out["person"].astype(str)
    cats = pd.Series(
        [_expense_category_for(d, p) for d, p in zip(desc, persons)],
        index=out.index,
    )
    is_refund = desc.map(_is_jyske_refund)
    is_karoline = persons.str.casefold().str.strip().eq(PERSON_KAROLINE)
    amt = (
        pd.to_numeric(out["amount_net"], errors="coerce")
        if "amount_net" in out.columns
        else pd.Series(np.nan, index=out.index)
    )
    is_refund = is_refund | (is_karoline & cats.notna() & amt.gt(0))
    is_personal = cats.isna() & ~desc.map(_is_jyske_non_spend) & amt.lt(0)
    keep = cats.notna() | is_refund | is_personal
    out = out.loc[keep].copy()
    if out.empty:
        return out

    idx = out.index
    out["type"] = np.where(is_refund.loc[idx], "refund", "expense")
    out["category"] = cats.loc[idx]
    out.loc[is_personal.loc[idx], "category"] = CATEGORY_PERSONAL
    return out.reset_index(drop=True)


def load_jyske_statement(
    csv_path: str | Path | None,
    person: str = PERSON_MEHDI,
) -> pd.DataFrame:
    """Load a Jyske CSV or PDF into the dashboard schema. Empty if path is missing."""
    if not csv_path:
        return pd.DataFrame()

    p = Path(csv_path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame()

    raw = _read_jyske_raw(p)
    if raw.empty:
        return pd.DataFrame()

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
    out["person"] = person
    out = out[out["completed_date"].notna() & out["amount_net"].notna() & out["description"].ne("")].copy()
    out = classify_included_jyske(out)
    return out.reset_index(drop=True)


def jyske_core_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Normalized Date/Text/Amount/Balance used to compare CSV vs PDF reads."""
    if df.empty:
        return pd.DataFrame(columns=["completed_date", "description", "amount", "balance", "_key"])

    desc_col = _resolve_column(df, COLUMN_ALIASES["description"])
    amt_col = _resolve_column(df, COLUMN_ALIASES["amount"])
    bal_col = _resolve_column(df, ("balance", "saldo"))
    out = pd.DataFrame(index=df.index)
    out["completed_date"] = _jyske_date_series(df)
    out["description"] = (
        df[desc_col].astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
        if desc_col
        else ""
    )
    out["amount"] = df[amt_col].map(parse_jyske_amount) if amt_col else np.nan
    out["balance"] = df[bal_col].map(parse_jyske_amount) if bal_col else np.nan
    out["_key"] = _jyske_dedup_key(df)
    return out.sort_values(
        ["completed_date", "description", "amount", "balance"], kind="stable"
    ).reset_index(drop=True)


def assert_jyske_csv_pdf_equivalent(
    csv_path: str | Path,
    pdf_path: str | Path,
) -> None:
    """Raise AssertionError if CSV and PDF parses differ in any core field."""
    csv_raw = _read_jyske_raw(Path(csv_path))
    pdf_raw = _read_jyske_raw(Path(pdf_path))
    csv_core = jyske_core_frame(csv_raw)
    pdf_core = jyske_core_frame(pdf_raw)

    if len(csv_core) != len(pdf_core):
        raise AssertionError(
            f"Row count differs: CSV={len(csv_core)} PDF={len(pdf_core)}"
        )

    csv_keys = csv_core["_key"].tolist()
    pdf_keys = pdf_core["_key"].tolist()
    if csv_keys != pdf_keys:
        only_csv = sorted(set(csv_keys) - set(pdf_keys))
        only_pdf = sorted(set(pdf_keys) - set(csv_keys))
        raise AssertionError(
            "Dedup keys differ.\n"
            f"only CSV ({len(only_csv)}): {only_csv[:8]}\n"
            f"only PDF ({len(only_pdf)}): {only_pdf[:8]}"
        )

    for col in ("amount", "balance"):
        if not np.allclose(
            csv_core[col].to_numpy(dtype=float),
            pdf_core[col].to_numpy(dtype=float),
            equal_nan=True,
            atol=1e-9,
            rtol=0,
        ):
            raise AssertionError(f"{col} values differ between CSV and PDF")

    if list(csv_core["description"]) != list(pdf_core["description"]):
        raise AssertionError("Descriptions differ between CSV and PDF")

    csv_loaded = load_jyske_statement(csv_path)
    pdf_loaded = load_jyske_statement(pdf_path)
    csv_dash = (
        csv_loaded[["completed_date", "description", "amount_net", "type", "category"]]
        .sort_values(["completed_date", "description", "amount_net"], kind="stable")
        .reset_index(drop=True)
        if not csv_loaded.empty
        else csv_loaded
    )
    pdf_dash = (
        pdf_loaded[["completed_date", "description", "amount_net", "type", "category"]]
        .sort_values(["completed_date", "description", "amount_net"], kind="stable")
        .reset_index(drop=True)
        if not pdf_loaded.empty
        else pdf_loaded
    )
    if not csv_dash.equals(pdf_dash):
        raise AssertionError(
            f"Dashboard load differs: CSV={len(csv_dash)} PDF={len(pdf_dash)}"
        )
