-- ═══════════════════════════════════════════════════
-- EDF Energy Prediction — RTE éco2mix multi-years data (batch + streaming)
-- Bronze (raw) = MinIO Parquet only — Silver -> Gold in PostgreSQL
-- ═══════════════════════════════════════════════════

SET search_path TO dw, public;

CREATE SCHEMA IF NOT EXISTS monitoring;

-- Silver: cleaned and enriched data table
CREATE TABLE IF NOT EXISTS dw.fact_consumption_silver (
    id                  BIGSERIAL       NOT NULL,
    datetime            TIMESTAMPTZ     NOT NULL,
    -- Validated consumption
    consumption_mw      NUMERIC(10,2),
    forecast_j1_mw      NUMERIC(10,2),
    forecast_error_mw   NUMERIC(10,2),       -- real - forecast difference
    forecast_error_pct  NUMERIC(6,3),
    -- Energy mix (all sources)
    nuclear_mw          NUMERIC(10,2),
    wind_mw             NUMERIC(10,2),
    solar_mw            NUMERIC(10,2),
    hydro_mw            NUMERIC(10,2),
    gas_mw              NUMERIC(10,2),
    bioenergy_mw        NUMERIC(10,2),
    fuel_mw             NUMERIC(10,2),
    coal_mw             NUMERIC(10,2),
    wind_onshore_mw     NUMERIC(10,2),
    wind_offshore_mw    NUMERIC(10,2),
    -- Exchanges
    physical_exchanges_mw NUMERIC(10,2),
    co2_rate              NUMERIC(8,2),
    -- Features temporal calculated
    hour                SMALLINT,
    day_of_week         SMALLINT,
    day_of_year         SMALLINT,
    week_of_year        SMALLINT,
    month               SMALLINT,
    year                SMALLINT,
    quarter             SMALLINT,
    season              SMALLINT,           -- 0=winter 1=spring 2=summer 3=autumn
    is_weekend          BOOLEAN,
    is_peak_hour        BOOLEAN,            -- 8h-20h working days
    -- Features engineered
    lag_1h_mw           NUMERIC(10,2),
    lag_24h_mw          NUMERIC(10,2),
    lag_168h_mw         NUMERIC(10,2),
    rolling_24h_mean    NUMERIC(10,2),
    rolling_24h_std     NUMERIC(10,2),
    renewable_share_pct NUMERIC(6,3),
    nuclear_share_pct   NUMERIC(6,3),
    -- Tempo EDF (EDF Tempo days)
    tempo_color         VARCHAR(10),        -- BLUE / WHITE / RED
    -- Data quality
    is_interpolated     BOOLEAN DEFAULT FALSE,
    quality_score       NUMERIC(4,3),       -- 0.0 to 1.0
    processed_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (datetime, id)
) PARTITION BY RANGE (datetime);

CREATE TABLE IF NOT EXISTS dw.fact_consumption_silver_2020
    PARTITION OF dw.fact_consumption_silver FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE IF NOT EXISTS dw.fact_consumption_silver_2021
    PARTITION OF dw.fact_consumption_silver FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
CREATE TABLE IF NOT EXISTS dw.fact_consumption_silver_2022
    PARTITION OF dw.fact_consumption_silver FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');
CREATE TABLE IF NOT EXISTS dw.fact_consumption_silver_2023
    PARTITION OF dw.fact_consumption_silver FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');
CREATE TABLE IF NOT EXISTS dw.fact_consumption_silver_2024
    PARTITION OF dw.fact_consumption_silver FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE IF NOT EXISTS dw.fact_consumption_silver_2025
    PARTITION OF dw.fact_consumption_silver FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS dw.fact_consumption_silver_2026
    PARTITION OF dw.fact_consumption_silver FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

CREATE UNIQUE INDEX IF NOT EXISTS uq_silver_datetime ON dw.fact_consumption_silver (datetime);

CREATE UNLOGGED TABLE IF NOT EXISTS dw.fact_consumption_silver_staging (
    LIKE dw.fact_consumption_silver INCLUDING DEFAULTS
);

CREATE INDEX IF NOT EXISTS idx_silver_datetime ON dw.fact_consumption_silver (datetime);
CREATE INDEX IF NOT EXISTS idx_silver_year_month ON dw.fact_consumption_silver (year, month);
CREATE INDEX IF NOT EXISTS idx_silver_tempo ON dw.fact_consumption_silver (tempo_color) WHERE tempo_color IS NOT NULL;

-- Gold: daily aggregates table
CREATE TABLE IF NOT EXISTS dw.agg_daily (
    date                DATE        PRIMARY KEY,
    year                SMALLINT    NOT NULL,
    month               SMALLINT    NOT NULL,
    day_of_week         SMALLINT,
    season              SMALLINT,
    is_weekend          BOOLEAN,
    tempo_color         VARCHAR(10),
    -- Consommation journalière
    consumption_mean_mw NUMERIC(10,2),
    consumption_min_mw  NUMERIC(10,2),
    consumption_max_mw  NUMERIC(10,2),
    consumption_std_mw  NUMERIC(10,2),
    consumption_total_gwh NUMERIC(12,3),    -- GWh = sum(MW * 0.5h) / 1000
    -- Pic
    peak_consumption_mw NUMERIC(10,2),
    peak_hour           SMALLINT,
    off_peak_mean_mw    NUMERIC(10,2),
    -- Prévision
    forecast_mae_mw     NUMERIC(10,2),
    forecast_mape_pct   NUMERIC(6,3),
    -- Mix moyen journalier
    nuclear_mean_mw     NUMERIC(10,2),
    nuclear_share_pct   NUMERIC(6,3),
    wind_mean_mw        NUMERIC(10,2),
    solar_mean_mw       NUMERIC(10,2),
    hydro_mean_mw       NUMERIC(10,2),
    renewable_share_pct NUMERIC(6,3),
    -- CO2
    co2_mean_gkwh       NUMERIC(8,2),
    co2_min_gkwh        NUMERIC(8,2),
    co2_max_gkwh        NUMERIC(8,2),
    -- Échanges
    net_export_gwh      NUMERIC(12,3),
    -- Data quality
    records_count       INT,
    interpolated_count  INT,
    quality_score_mean  NUMERIC(4,3),
    computed_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agg_daily_year_month ON dw.agg_daily (year, month);
CREATE INDEX IF NOT EXISTS idx_agg_daily_season ON dw.agg_daily (season);

CREATE UNLOGGED TABLE IF NOT EXISTS dw.agg_daily_staging (
    LIKE dw.agg_daily INCLUDING DEFAULTS
);

-- Gold: monthly aggregates table
CREATE TABLE IF NOT EXISTS dw.agg_monthly (
    year                    SMALLINT,
    month                   SMALLINT,
    season                  SMALLINT,
    -- Consommation
    consumption_mean_mw     NUMERIC(10,2),
    consumption_total_twh   NUMERIC(12,4),
    consumption_yoy_pct     NUMERIC(6,3),   -- variation année sur année
    -- Production
    nuclear_total_gwh       NUMERIC(12,3),
    wind_total_gwh          NUMERIC(12,3),
    solar_total_gwh         NUMERIC(12,3),
    hydro_total_gwh         NUMERIC(12,3),
    renewable_pct           NUMERIC(6,3),
    -- CO2
    co2_mean_gkwh           NUMERIC(8,2),
    -- Jours Tempo
    tempo_bleu_days         SMALLINT,
    tempo_blanc_days        SMALLINT,
    tempo_rouge_days        SMALLINT,
    -- Records
    trading_days            SMALLINT,
    computed_at             TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (year, month)
);

CREATE UNLOGGED TABLE IF NOT EXISTS dw.agg_monthly_staging (
    LIKE dw.agg_monthly INCLUDING DEFAULTS
);

-- ETL metadata table
CREATE TABLE IF NOT EXISTS etl.pipeline_runs (
    run_id          UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
    dag_id          VARCHAR(200)    NOT NULL,
    run_type        VARCHAR(50),    -- 'ingest', 'transform', 'aggregate'
    source_file     VARCHAR(500),
    status          VARCHAR(50)     NOT NULL DEFAULT 'running',
    started_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    rows_read       BIGINT,
    rows_written    BIGINT,
    rows_skipped    BIGINT,
    rows_errored    BIGINT,
    bronze_path     VARCHAR(500),   -- MinIO path
    silver_path     VARCHAR(500),
    gold_path       VARCHAR(500),
    error_message   TEXT,
    metadata        JSONB
);

CREATE TABLE IF NOT EXISTS etl.data_quality_checks (
    id              BIGSERIAL   PRIMARY KEY,
    run_id          UUID        REFERENCES etl.pipeline_runs(run_id),
    check_name      VARCHAR(100) NOT NULL,
    table_name      VARCHAR(100),
    column_name     VARCHAR(100),
    check_type      VARCHAR(50),  -- 'null_check' | 'range_check' | 'uniqueness' | 'freshness'
    expected_value  NUMERIC,
    actual_value    NUMERIC,
    passed          BOOLEAN,
    detail          TEXT,
    checked_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS etl.model_metrics (
    id              BIGSERIAL PRIMARY KEY,
    run_id          VARCHAR(50),
    model_name      VARCHAR(100) NOT NULL,
    rmse            DOUBLE PRECISION,
    mae             DOUBLE PRECISION,
    mape_pct        DOUBLE PRECISION,
    r2              DOUBLE PRECISION,
    train_time_s    DOUBLE PRECISION,
    trained_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_model_metrics_trained_at
    ON etl.model_metrics (trained_at DESC);
CREATE INDEX IF NOT EXISTS idx_model_metrics_model
    ON etl.model_metrics (model_name);

-- Analytical views
CREATE OR REPLACE VIEW dw.v_consumption_last_30_days AS
SELECT
    datetime,
    consumption_mw,
    forecast_j1_mw,
    forecast_error_mw,
    nuclear_mw,
    wind_mw,
    solar_mw,
    hydro_mw,
    renewable_share_pct,
    co2_rate,
    tempo_color,
    hour,
    is_weekend,
    is_peak_hour
FROM dw.fact_consumption_silver
WHERE datetime >= NOW() - INTERVAL '30 days'
ORDER BY datetime DESC;

CREATE OR REPLACE VIEW dw.v_annual_summary AS
WITH daily_by_year AS (
    SELECT
        year,
        SUM(consumption_total_gwh) / 1000      AS consumption_twh,
        AVG(nuclear_share_pct)                 AS nuclear_share_pct,
        AVG(renewable_share_pct)               AS renewable_share_pct,
        AVG(co2_mean_gkwh)                     AS co2_mean_gkwh,
        AVG(forecast_mae_mw)                   AS forecast_mae_mw
    FROM dw.agg_daily
    GROUP BY year
),
monthly_by_year AS (
    SELECT
        year,
        SUM(tempo_bleu_days)                   AS tempo_bleu_days,
        SUM(tempo_blanc_days)                  AS tempo_blanc_days,
        SUM(tempo_rouge_days)                  AS tempo_rouge_days
    FROM dw.agg_monthly
    GROUP BY year
)
SELECT
    COALESCE(d.year, m.year)                   AS year,
    d.consumption_twh,
    d.nuclear_share_pct,
    d.renewable_share_pct,
    d.co2_mean_gkwh,
    d.forecast_mae_mw,
    m.tempo_bleu_days,
    m.tempo_blanc_days,
    m.tempo_rouge_days
FROM daily_by_year d
FULL OUTER JOIN monthly_by_year m USING (year)
ORDER BY year;

CREATE OR REPLACE VIEW dw.v_etl_pipeline_status AS
SELECT
    dag_id,
    status,
    COUNT(*)            AS run_count,
    AVG(rows_written)   AS avg_rows,
    MAX(started_at)     AS last_run,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failures
FROM etl.pipeline_runs
GROUP BY dag_id, status
ORDER BY last_run DESC;

-- Permissions
GRANT ALL ON SCHEMA dw, etl, monitoring TO edf;
GRANT ALL ON ALL TABLES IN SCHEMA dw TO edf;
GRANT ALL ON ALL TABLES IN SCHEMA etl TO edf;
GRANT ALL ON ALL TABLES IN SCHEMA monitoring TO edf;
GRANT ALL ON ALL SEQUENCES IN SCHEMA dw TO edf;
GRANT ALL ON ALL SEQUENCES IN SCHEMA etl TO edf;
GRANT ALL ON ALL SEQUENCES IN SCHEMA monitoring TO edf;
