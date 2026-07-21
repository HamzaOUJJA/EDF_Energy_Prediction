#!/usr/bin/env python3
# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# scripts/generate_report_eda.py — CLI wrapper (logic in spark.common.eda_report).
# =======================================================================

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from spark.common.eda_report import (  # noqa: E402
    generate_data_charts,
    generate_ml_charts,
    report_output_dir,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the professional EDA report.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--data-only", action="store_true", help="Data charts only")
    group.add_argument("--ml-only", action="store_true", help="ML chart only")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    out = report_output_dir()
    charts: list[str] = []

    if args.ml_only:
        paths = generate_ml_charts(out)
        charts.extend(paths)
    elif args.data_only:
        charts.extend(generate_data_charts(out))
    else:
        charts.extend(generate_data_charts(out))
        paths = generate_ml_charts(out)
        charts.extend(paths)

    print(f"\nProfessional report — {len(charts)} PNG chart(s) in {out}/")


if __name__ == "__main__":
    main()
