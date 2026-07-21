# =======================================================================
# **************    Projet : EDF Energy Prediction         **************
# **************    Version : 1.0.0                        **************
# =======================================================================
#
# test_etl_pipeline.py — Tests pipeline ETL RTE
# =======================================================================

import pytest


# Test RTE parser
class TestRTEParser:
    # Test parse XLS row valid
    def test_parse_xls_row_valid(self):
        from rte_pipeline.producers.rte_producer import parse_xls_row
        headers = [
            "Périmètre", "Nature", "Date", "Heures",
            "Consommation", "Prévision J-1", "Nucléaire",
            "Eolien", "Taux de Co2",
        ]
        values = ["France", "Données définitives", "2024-01-01", "00:00",
                  "55239", "55000", "39886", "15557", "20"]

        record = parse_xls_row(headers, values, "test.xls")
        assert record is not None
        assert record["consumption_mw"] == 55239.0
        assert record["nuclear_mw"] == 39886.0
        assert record["co2_rate"] == 20.0

    # Test parse XLS row empty values
    def test_parse_xls_row_empty_values(self):
        from rte_pipeline.producers.rte_producer import parse_xls_row

        headers = ["Périmètre", "Nature", "Date", "Heures", "Consommation"]
        values = ["France", "Données", "2024-01-01", "00:15", "ND"]

        record = parse_xls_row(headers, values, "test.xls")
        assert record is not None
        assert record.get("consumption_mw") is None

    # Test parse XLS row datetime built
    def test_parse_xls_row_datetime_built(self):
        from rte_pipeline.producers.rte_producer import parse_xls_row

        headers = ["Périmètre", "Nature", "Date", "Heures", "Consommation"]
        values = ["France", "Données définitives", "2024-06-15", "14:30", "48000"]

        record = parse_xls_row(headers, values, "test.xls")
        assert record is not None
        assert "datetime" in record
        assert "2024-06-15" in record["datetime"]
        assert "14:30" in record["datetime"]

    # Test parse XLS row invalid skipped
    def test_parse_xls_row_invalid_skipped(self):
        from rte_pipeline.producers.rte_producer import parse_xls_row

        # Ligne trop courte
        record = parse_xls_row(["A", "B"], ["x"], "test.xls")
        assert record is None

    # Test iter XLS file
    def test_iter_xls_file(self, tmp_data_dir):
        from rte_pipeline.producers.rte_producer import iter_xls_file

        filepath = str(tmp_data_dir / "eCO2mix_RTE_Annuel-Definitif_2024.xls")
        records = list(iter_xls_file(filepath))

        assert len(records) == 3
        assert records[0]["consumption_mw"] == 55239.0
        assert records[0]["nuclear_mw"] == 39886.0

    # Test iter tempo file
    def test_iter_tempo_file(self, tmp_data_dir):
        from rte_pipeline.producers.rte_producer import iter_tempo_file

        filepath = str(tmp_data_dir / "eCO2mix_RTE_tempo_2024-2025.xls")
        records = list(iter_tempo_file(filepath))

        assert len(records) == 4
        colors = {r["tempo_color"] for r in records}
        assert colors == {"BLEU", "BLANC", "ROUGE"}


# Test Parquet Conversion
class TestParquetConversion:
    # Test records to parquet RTE
    def test_records_to_parquet_rte(self):
        import pyarrow.parquet as pq
        import io
        from rte_pipeline.kafka_parquet import records_to_parquet, RTE_SCHEMA

        records = [
            {
                "datetime": "2024-01-01T00:00:00+00:00",
                "consumption_mw": 55239.0,
                "nuclear_mw": 39886.0,
                "wind_mw": 15557.0,
                "co2_rate": 20.0,
            }
        ]

        parquet_bytes = records_to_parquet(records, RTE_SCHEMA)
        assert len(parquet_bytes) > 0

        # Relire le Parquet
        buf = io.BytesIO(parquet_bytes)
        table = pq.read_table(buf)
        assert table.num_rows == 1

    # Test records to parquet empty
    def test_records_to_parquet_empty(self):
        from rte_pipeline.kafka_parquet import records_to_parquet, RTE_SCHEMA

        result = records_to_parquet([], RTE_SCHEMA)
        assert result == b""

    # Test records to parquet tempo
    def test_records_to_parquet_tempo(self):
        import pyarrow.parquet as pq
        import io
        from rte_pipeline.kafka_parquet import records_to_parquet, TEMPO_SCHEMA

        records = [
            {"date": "2024-01-01", "tempo_color": "BLEU", "source_file": "test.xls"},
            {"date": "2024-01-02", "tempo_color": "ROUGE", "source_file": "test.xls"},
        ]

        parquet_bytes = records_to_parquet(records, TEMPO_SCHEMA)
        buf = io.BytesIO(parquet_bytes)
        table = pq.read_table(buf)
        assert table.num_rows == 2

    # Test tempo color validation
    def test_tempo_color_validation(self):
        valid_colors = {"BLEU", "BLANC", "ROUGE"}
        test_colors = ["BLEU", "ROUGE", "BLEU", "BLANC", "VERT", None]

        invalid = [c for c in test_colors if c and c not in valid_colors]
        assert invalid == ["VERT"]

    # Test s3 key streaming
    def test_s3_key_streaming(self):
        from rte_pipeline.kafka_parquet import build_streaming_s3_key

        key = build_streaming_s3_key("rte.raw", "2024-01-15")
        assert key == "rte/streaming/rte/raw/2024-01-15/data.parquet"

    # Test s3 key streaming tempo
    def test_s3_key_streaming_tempo(self):
        from rte_pipeline.kafka_parquet import build_streaming_s3_key

        key = build_streaming_s3_key("rte.tempo", "2024-06-01")
        assert key == "rte/streaming/rte/tempo/2024-06-01/data.parquet"
