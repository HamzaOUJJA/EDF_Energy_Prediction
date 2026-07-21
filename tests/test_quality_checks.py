# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# test_quality_checks.py — Tests contrôles qualité partagés (sources XLS + post-ETL)
# =======================================================================

from __future__ import annotations

import pytest

from edf_pipeline.quality import (
    allow_year_gaps,
    find_year_gaps,
    get_monitoring_checks,
    get_post_etl_checks,
    get_streaming_daily_checks,
    validate_xls_sources,
    validate_xls_sources_or_raise,
)

# Test find year gaps
class TestFindYearGaps:
    # Test no gap
    def test_no_gap(self):
        assert find_year_gaps({2023, 2024, 2025}) == []

    # Test single year
    def test_single_year(self):
        assert find_year_gaps({2024}) == []

    # Test missing middle year
    def test_missing_middle_year(self):
        assert find_year_gaps({2024, 2026}) == [2025]

    # Test empty
    def test_empty(self):
        assert find_year_gaps(set()) == []

# Test validate XLS sources
class TestValidateXlsSources:
    # Test detects consumption year gap
    def test_detects_consumption_year_gap(self, tmp_data_dir):
        summary = validate_xls_sources(str(tmp_data_dir))
        assert summary["years_covered"] == [2024]
        assert summary["consumption_year_gaps"] == []
        assert summary["passed"] is True

    # Test raises on year gap
    def test_raises_on_year_gap(self, tmp_path, sample_xls_content, monkeypatch):
        monkeypatch.setenv("EDF_ENVIRONMENT", "prod")
        monkeypatch.delenv("QUALITY_ALLOW_YEAR_GAPS", raising=False)
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()

        (raw_dir / "eCO2mix_RTE_Annuel-Definitif_2024.xls").write_bytes(
            sample_xls_content
        )
        content_2026 = sample_xls_content.replace(b"2024-", b"2026-")
        (raw_dir / "eCO2mix_RTE_En-cours-TR.xls").write_bytes(content_2026)

        with pytest.raises(ValueError, match="2025"):
            validate_xls_sources_or_raise(str(raw_dir))

    # Test allows year gap in dev
    def test_allows_year_gap_in_dev(self, tmp_path, sample_xls_content, monkeypatch):
        monkeypatch.setenv("EDF_ENVIRONMENT", "dev")
        monkeypatch.delenv("QUALITY_ALLOW_YEAR_GAPS", raising=False)
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()

        (raw_dir / "eCO2mix_RTE_Annuel-Definitif_2024.xls").write_bytes(
            sample_xls_content
        )
        content_2026 = sample_xls_content.replace(b"2024-", b"2026-")
        (raw_dir / "eCO2mix_RTE_En-cours-TR.xls").write_bytes(content_2026)

        summary = validate_xls_sources_or_raise(str(raw_dir))
        assert summary["consumption_year_gaps"] == [2025]

    # Test post-ETL year gap is warning in dev
    def test_post_etl_year_gap_is_warning_in_dev(self, monkeypatch):
        monkeypatch.setenv("EDF_ENVIRONMENT", "dev")
        checks = get_post_etl_checks()
        year_check = next(c for c in checks if c["name"] == "year_coverage_gaps")
        assert year_check["severity"] == "warning"
        assert allow_year_gaps() is True

    # Test missing directory
    def test_missing_directory(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            validate_xls_sources(str(tmp_path / "missing"))


# Test quality check catalog
class TestQualityCheckCatalog:
    # Test monitoring checks unique names
    def test_monitoring_checks_unique_names(self):
        names = [c["name"] for c in get_monitoring_checks()]
        assert len(names) == len(set(names))
        assert len(names) >= 8

    # Test streaming daily checks use run date
    def test_streaming_daily_checks_use_run_date(self):
        checks = get_streaming_daily_checks("2024-03-15")
        assert len(checks) == 3
        assert all("2024-03-15" in c["sql"] for c in checks)
