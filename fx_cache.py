from __future__ import annotations

from datetime import date as Date
from datetime import timedelta
from pathlib import Path
import logging
import threading
import time
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

FX_CACHE_CURRENCIES: tuple[str, ...] = ("USD", "EUR", "GBP")
FX_CACHE_TO_CCY = "DKK"
FX_CACHE_START_DATE = Date(2025, 12, 1)

_fx_session: Optional[requests.Session] = None
_fx_cache_lock = threading.Lock()
_fx_series_cache: dict[tuple[str, str, str], tuple[float, pd.Series]] = {}


def get_fx_session() -> requests.Session:
    """Shared requests session with retries for resilience."""

    global _fx_session
    if _fx_session is not None:
        return _fx_session

    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    _fx_session = session
    return session


def fx_rate_on_date(
    date: pd.Timestamp,
    from_ccy: str,
    to_ccy: str = FX_CACHE_TO_CCY,
    max_backtrack_days: int = 10,
    _cache: Dict[Tuple[str, str, str], Tuple[Optional[float], Optional[pd.Timestamp]]] = {},
) -> Tuple[Optional[float], Optional[pd.Timestamp]]:
    """Returns (rate, used_date) using frankfurter.app with weekend/holiday backtracking."""

    from_ccy = str(from_ccy).upper().strip()
    to_ccy = str(to_ccy).upper().strip()

    if not from_ccy or from_ccy == to_ccy:
        used = pd.Timestamp(date).date() if not pd.isna(date) else None
        return 1.0, used

    if pd.isna(date):
        return None, None

    d = pd.Timestamp(date).date()
    last_error: Optional[Exception] = None
    for _attempt in range(max_backtrack_days + 1):
        key = (str(d), from_ccy, to_ccy)
        if key in _cache:
            return _cache[key]

        url = f"https://api.frankfurter.app/{d}?from={from_ccy}&to={to_ccy}"
        try:
            r = get_fx_session().get(url, timeout=8)
            if r.status_code == 200:
                data = r.json()
                rate = float(data["rates"][to_ccy])
                api_date_str = data.get("date")
                used_date = pd.to_datetime(api_date_str).date() if api_date_str else d
                _cache[key] = (rate, used_date)
                return rate, used_date
        except Exception as e:
            last_error = e

        d = (pd.Timestamp(d) - pd.Timedelta(days=1)).date()
        time.sleep(0.05)

    if last_error is not None:
        logger.warning(
            f"Failed to fetch FX rate for {from_ccy}->{to_ccy} starting at {pd.Timestamp(date).date()} "
            f"after {max_backtrack_days+1} attempts. Last error: {last_error}"
        )
    return None, None


def _fx_cache_path(data_dir: str | Path, from_ccy: str, to_ccy: str = FX_CACHE_TO_CCY) -> Path:
    base = Path(data_dir)
    return base / f"fx_{from_ccy.upper()}_{to_ccy.upper()}.csv"


def _read_fx_cache_csv(path: Path) -> pd.Series:
    df = pd.read_csv(path)
    if df.empty or "date" not in df.columns or "rate" not in df.columns:
        return pd.Series(dtype="float")

    dt = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    rate = pd.to_numeric(df["rate"], errors="coerce")
    s = pd.Series(rate.values, index=dt)
    s = s[s.index.notna()]
    s = s[~s.index.duplicated(keep="last")]
    return s.sort_index()


def _write_fx_cache_csv(path: Path, series: pd.Series) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    s = series.copy()
    s.index = pd.to_datetime(s.index, errors="coerce").normalize()
    s = s[s.index.notna()].sort_index()
    df = pd.DataFrame({"date": s.index.strftime("%Y-%m-%d"), "rate": s.values})

    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(path)


def _fetch_fx_timeseries(from_ccy: str, to_ccy: str, start: Date, end: Date) -> pd.Series:
    from_ccy = str(from_ccy).upper().strip()
    to_ccy = str(to_ccy).upper().strip()
    start_s = pd.Timestamp(start).strftime("%Y-%m-%d")
    end_s = pd.Timestamp(end).strftime("%Y-%m-%d")
    url = f"https://api.frankfurter.app/{start_s}..{end_s}?from={from_ccy}&to={to_ccy}"

    r = get_fx_session().get(url, timeout=12)
    r.raise_for_status()
    data = r.json()
    rates = data.get("rates", {})

    rows: list[tuple[pd.Timestamp, float]] = []
    for day_str, obj in rates.items():
        try:
            v = obj.get(to_ccy)
            if v is None:
                continue
            rows.append((pd.to_datetime(day_str).normalize(), float(v)))
        except Exception:
            continue

    if not rows:
        return pd.Series(dtype="float")

    idx = [d for d, _ in rows]
    vals = [v for _, v in rows]
    s = pd.Series(vals, index=pd.DatetimeIndex(idx)).sort_index()
    s = s[~s.index.duplicated(keep="last")]
    return s


def _daily_filled_series(series: pd.Series, start: Date, end: Date) -> pd.Series:
    if series.empty:
        return series

    full_idx = pd.date_range(pd.Timestamp(start), pd.Timestamp(end), freq="D")
    s = series.copy()
    s.index = pd.to_datetime(s.index, errors="coerce").normalize()
    s = s[s.index.notna()].sort_index()
    s = s.reindex(full_idx)
    return s.bfill().ffill()


def ensure_fx_cache_files(
    data_dir: str | Path = "data",
    currencies: Iterable[str] = FX_CACHE_CURRENCIES,
    start_date: Date = FX_CACHE_START_DATE,
    to_ccy: str = FX_CACHE_TO_CCY,
    max_wait_seconds: int = 180,
    retry_sleep_seconds: float = 5.0,
) -> None:
    """Ensure local FX cache CSVs exist (first run blocks until downloaded).

    If the FX API is slow/down, this will keep retrying for up to max_wait_seconds
    before raising.
    """

    today = pd.Timestamp.today().date()
    for from_ccy in currencies:
        from_ccy = str(from_ccy).upper().strip()
        if not from_ccy:
            continue

        path = _fx_cache_path(data_dir, from_ccy, to_ccy)
        if path.exists():
            continue

        deadline = time.monotonic() + max_wait_seconds
        last_error: Exception | None = None
        while True:
            try:
                fetched = _fetch_fx_timeseries(from_ccy, to_ccy, start_date, today)
                filled = _daily_filled_series(fetched, start_date, today)
                _write_fx_cache_csv(path, filled)
                break
            except Exception as e:
                last_error = e
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        f"Failed to initialize FX cache for {from_ccy}->{to_ccy} after waiting {max_wait_seconds}s: {e}"
                    )
                time.sleep(retry_sleep_seconds)


def _update_one_fx_cache_file(
    data_dir: str | Path,
    from_ccy: str,
    start_date: Date,
    to_ccy: str,
) -> bool:
    path = _fx_cache_path(data_dir, from_ccy, to_ccy)
    if not path.exists():
        return False

    today = pd.Timestamp.today().date()

    with _fx_cache_lock:
        existing = _read_fx_cache_csv(path)

    if existing.empty:
        fetched = _fetch_fx_timeseries(from_ccy, to_ccy, start_date, today)
        filled = _daily_filled_series(fetched, start_date, today)
        with _fx_cache_lock:
            _write_fx_cache_csv(path, filled)
        return True

    last = pd.Timestamp(existing.index.max()).date()
    if last >= today:
        return False

    fetch_start = max(start_date, last - timedelta(days=2))
    fetched = _fetch_fx_timeseries(from_ccy, to_ccy, fetch_start, today)

    combined = pd.concat([existing, fetched]).sort_index()
    combined = combined[~combined.index.duplicated(keep="last")]
    filled = _daily_filled_series(combined, start_date, today)

    with _fx_cache_lock:
        _write_fx_cache_csv(path, filled)

    key = (str(Path(data_dir)), from_ccy.upper(), to_ccy.upper())
    _fx_series_cache.pop(key, None)
    return True


class FxCacheBackgroundUpdater:
    """Background updater that refreshes FX cache files to today's date."""

    def __init__(
        self,
        data_dir: str | Path = "data",
        currencies: Iterable[str] = FX_CACHE_CURRENCIES,
        start_date: Date = FX_CACHE_START_DATE,
        to_ccy: str = FX_CACHE_TO_CCY,
    ) -> None:
        self.data_dir = str(data_dir)
        self.currencies = tuple(str(c).upper().strip() for c in currencies)
        self.start_date = start_date
        self.to_ccy = str(to_ccy).upper().strip()

        self.done = threading.Event()
        self.updated = False
        self.error: str | None = None

        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> "FxCacheBackgroundUpdater":
        self._thread.start()
        return self

    def _run(self) -> None:
        try:
            any_updated = False
            for c in self.currencies:
                if not c:
                    continue
                try:
                    any_updated |= _update_one_fx_cache_file(
                        self.data_dir, c, self.start_date, self.to_ccy
                    )
                except Exception as e:
                    self.error = str(e)
            self.updated = any_updated
        finally:
            self.done.set()


def load_fx_cache_series(
    from_ccy: str,
    data_dir: str | Path = "data",
    to_ccy: str = FX_CACHE_TO_CCY,
) -> pd.Series:
    """Load a cached FX series from disk, with mtime-based in-memory caching."""

    from_ccy = str(from_ccy).upper().strip()
    to_ccy = str(to_ccy).upper().strip()
    path = _fx_cache_path(data_dir, from_ccy, to_ccy)
    if not path.exists():
        return pd.Series(dtype="float")

    mtime = path.stat().st_mtime
    key = (str(Path(data_dir)), from_ccy, to_ccy)
    cached = _fx_series_cache.get(key)
    if cached is not None and cached[0] == mtime:
        return cached[1]

    with _fx_cache_lock:
        s = _read_fx_cache_csv(path)

    _fx_series_cache[key] = (mtime, s)
    return s


def fx_cache_version(
    data_dir: str | Path = "data",
    currencies: Iterable[str] = FX_CACHE_CURRENCIES,
    to_ccy: str = FX_CACHE_TO_CCY,
) -> float:
    """Return a single float that changes whenever any FX cache file changes."""

    base = Path(data_dir)
    mtimes: list[float] = []
    for c in currencies:
        c = str(c).upper().strip()
        if not c:
            continue
        p = _fx_cache_path(base, c, to_ccy)
        if p.exists():
            try:
                mtimes.append(p.stat().st_mtime)
            except Exception:
                continue
    return max(mtimes) if mtimes else 0.0
