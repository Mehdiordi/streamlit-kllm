"""Assert Jyske PDF and CSV parsers produce the same core rows."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from jyske_processing import (  # noqa: E402
    DEFAULT_SEARCH_DIRS,
    assert_jyske_csv_pdf_equivalent,
    ensure_jyske_reference_merged,
    find_jyske_reference_csv,
)


def _paired_exports(search_dirs: list[str]) -> list[tuple[Path, Path]]:
    pairs: list[tuple[Path, Path]] = []
    seen: set[str] = set()
    for folder in search_dirs:
        base = Path(folder)
        if not base.is_dir():
            continue
        for csv_path in base.glob("*.csv"):
            pdf_path = csv_path.with_suffix(".pdf")
            if not pdf_path.exists():
                continue
            key = csv_path.stem.casefold()
            if key in seen:
                continue
            seen.add(key)
            pairs.append((csv_path, pdf_path))
    return pairs


def main() -> int:
    dirs = [str(d) for d in DEFAULT_SEARCH_DIRS]
    pairs = _paired_exports(dirs)
    if not pairs:
        print("No CSV+PDF Jyske pairs found.")
        return 1

    reference_before = find_jyske_reference_csv(dirs)
    ref_mtime_before = reference_before.stat().st_mtime if reference_before else None

    for csv_path, pdf_path in pairs:
        print(f"Comparing {csv_path.name} ↔ {pdf_path.name}")
        assert_jyske_csv_pdf_equivalent(csv_path, pdf_path)
        print("  raw rows and dashboard load match")

    merged = ensure_jyske_reference_merged(dirs)
    if merged is None or not Path(merged.path).exists():
        raise AssertionError("Jyske reference is missing after merge")
    if "jyske_reference" not in Path(merged.path).name.casefold() and reference_before:
        raise AssertionError(f"Reference path changed unexpectedly: {merged.path}")
    if ref_mtime_before is not None and not Path(merged.path).exists():
        raise AssertionError("Reference file was deleted")

    print(
        f"Reference preserved: {merged.path} "
        f"({merged.min_date} → {merged.max_date}, {merged.row_count} rows, +{merged.added_rows})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
