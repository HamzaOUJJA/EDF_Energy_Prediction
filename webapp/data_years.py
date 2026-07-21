# =======================================================================
# webapp/data_years.py — Discover which years are covered by the RTE XLS
# files in DATA_DIR, for the year selector.
# =======================================================================

from __future__ import annotations

import sys
from pathlib import Path

from config import DATA_DIR, REPO_ROOT

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def list_available_years(data_dir: Path | None = None) -> list[int]:
    """Years covered by the non-tempo XLS files in ``data_dir``.

    Derived from parsed record datetimes (same approach as
    ``edf_pipeline.quality.validate_xls_sources``), not filename guessing —
    a single file can span multiple years.
    """
    from rte_pipeline.parsing import parse_xls_file

    data_path = data_dir or DATA_DIR
    if not data_path.is_dir():
        return []

    years: set[int] = set()
    xls_files = (
        f for f in data_path.glob("eCO2mix_RTE_*.xls")
        if "tempo" not in f.name.lower()
    )
    for f in xls_files:
        for record in parse_xls_file(str(f)):
            dt = record.get("datetime")
            if dt:
                years.add(int(str(dt)[:4]))

    return sorted(years)
