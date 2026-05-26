-- ================================================================
-- PostgreSQL Init Script — Data Platform
-- Runs automatically on first container startup
-- ================================================================

-- ── OpenMetadata (Data Governance) ───────────────────────────────
CREATE USER openmetadata WITH PASSWORD 'openmetadata';
CREATE DATABASE openmetadata OWNER openmetadata;
GRANT ALL PRIVILEGES ON DATABASE openmetadata TO openmetadata;

-- ── Airflow (Orchestration Metadata) ─────────────────────────────
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- ── Superset (BI Tool Metadata) ───────────────────────────────────
CREATE USER superset WITH PASSWORD 'superset';
CREATE DATABASE superset OWNER superset;
GRANT ALL PRIVILEGES ON DATABASE superset TO superset;
