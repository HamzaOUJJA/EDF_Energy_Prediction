-- ═══════════════════════════════════════════════════════════════
-- EDF Energy Prediction — Initialisation PostgreSQL
-- ═══════════════════════════════════════════════════════════════
-- Executed ONLY ONCE at first container startup
-- (docker-entrypoint-initdb.d). The DW schema is applied afterwards
-- by ``make init-postgres-schema`` / Airflow ``init_postgres_schema`` task.
--
-- Schema convention:
--   • public : used by Airflow for its metadata (dag, task_instance,
--              connection, variable, xcom…). Do NOT add business tables here.
--   • dw     : Data Warehouse (facts + dimensions + Gold aggregates).
--   • etl    : ETL/ML pipelines metadata
--              (pipeline_runs, data_quality_checks, model_metrics).
--   • monitoring : reserved for observability / quality metrics (Grafana).
-- ═══════════════════════════════════════════════════════════════

-- ── Database extensions ─────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";   -- run_id UUID dans etl.pipeline_runs
CREATE EXTENSION IF NOT EXISTS "pg_trgm";     -- fuzzy search (on dim_*)
CREATE EXTENSION IF NOT EXISTS "btree_gist";  -- range constraints (datetime)

-- ── Business schemas ─────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS etl;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS monitoring;
