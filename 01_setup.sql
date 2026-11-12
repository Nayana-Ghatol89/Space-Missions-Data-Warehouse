CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS CURATED;
CREATE SCHEMA IF NOT EXISTS OPS;

CREATE SEQUENCE IF NOT EXISTS CURATED.SEQ_ROCKET_KEY START 1;
CREATE SEQUENCE IF NOT EXISTS CURATED.SEQ_LAUNCH_SITE_KEY START 1;
CREATE SEQUENCE IF NOT EXISTS CURATED.SEQ_AGENCY_KEY START 1;
CREATE SEQUENCE IF NOT EXISTS CURATED.SEQ_MISSION_TYPE_KEY START 1;
CREATE SEQUENCE IF NOT EXISTS CURATED.SEQ_ORBIT_KEY START 1;
CREATE SEQUENCE IF NOT EXISTS CURATED.SEQ_DATE_KEY START 1;
CREATE SEQUENCE IF NOT EXISTS CURATED.SEQ_LAUNCH_KEY START 1;
CREATE SEQUENCE IF NOT EXISTS OPS.SEQ_AUDIT_KEY START 1;

CREATE TABLE IF NOT EXISTS RAW.API_PAYLOADS (
  run_id VARCHAR NOT NULL,
  source_name VARCHAR NOT NULL,
  payload_index BIGINT NOT NULL,
  fetched_at TIMESTAMPTZ,
  payload_json JSON NOT NULL,
  source_file VARCHAR NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
  PRIMARY KEY (run_id, source_name, payload_index)
);

CREATE TABLE IF NOT EXISTS RAW.LAUNCHES (
  run_id VARCHAR NOT NULL,
  source_name VARCHAR NOT NULL,
  source_id VARCHAR NOT NULL,
  company VARCHAR,
  location VARCHAR,
  launch_datetime VARCHAR,
  rocket_name VARCHAR,
  mission_name VARCHAR,
  rocket_status VARCHAR,
  mission_status VARCHAR,
  mission_type VARCHAR,
  orbit VARCHAR,
  agency VARCHAR,
  payload_mass_kg VARCHAR,
  source_file VARCHAR NOT NULL,
  loaded_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
  PRIMARY KEY (run_id, source_name, source_id)
);

CREATE TABLE IF NOT EXISTS CURATED.DIM_ROCKET (
  rocket_key BIGINT PRIMARY KEY,
  rocket_name VARCHAR NOT NULL,
  family_name VARCHAR NOT NULL,
  parent_rocket_name VARCHAR,
  parent_rocket_key BIGINT,
  attribute_hash VARCHAR NOT NULL,
  effective_from TIMESTAMPTZ NOT NULL,
  effective_to TIMESTAMPTZ,
  is_current BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS CURATED.DIM_LAUNCH_SITE (
  launch_site_key BIGINT PRIMARY KEY,
  site_name VARCHAR NOT NULL,
  attribute_hash VARCHAR NOT NULL,
  effective_from TIMESTAMPTZ NOT NULL,
  effective_to TIMESTAMPTZ,
  is_current BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS CURATED.DIM_AGENCY (
  agency_key BIGINT PRIMARY KEY,
  agency_name VARCHAR NOT NULL,
  attribute_hash VARCHAR NOT NULL,
  effective_from TIMESTAMPTZ NOT NULL,
  effective_to TIMESTAMPTZ,
  is_current BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS CURATED.DIM_MISSION_TYPE (
  mission_type_key BIGINT PRIMARY KEY,
  mission_type_name VARCHAR NOT NULL,
  attribute_hash VARCHAR NOT NULL,
  effective_from TIMESTAMPTZ NOT NULL,
  effective_to TIMESTAMPTZ,
  is_current BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS CURATED.DIM_ORBIT (
  orbit_key BIGINT PRIMARY KEY,
  orbit_name VARCHAR NOT NULL,
  attribute_hash VARCHAR NOT NULL,
  effective_from TIMESTAMPTZ NOT NULL,
  effective_to TIMESTAMPTZ,
  is_current BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS CURATED.DIM_DATE (
  date_key BIGINT PRIMARY KEY,
  full_date DATE NOT NULL UNIQUE,
  calendar_year INTEGER NOT NULL,
  calendar_quarter INTEGER NOT NULL,
  month_number INTEGER NOT NULL,
  month_name VARCHAR NOT NULL,
  day_of_month INTEGER NOT NULL,
  day_of_week INTEGER NOT NULL,
  is_weekend BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS CURATED.FACT_LAUNCH (
  launch_key BIGINT PRIMARY KEY,
  source_name VARCHAR NOT NULL,
  source_id VARCHAR NOT NULL,
  launch_name VARCHAR,
  launch_timestamp TIMESTAMP,
  rocket_key BIGINT NOT NULL REFERENCES CURATED.DIM_ROCKET(rocket_key),
  launch_site_key BIGINT NOT NULL REFERENCES CURATED.DIM_LAUNCH_SITE(launch_site_key),
  agency_key BIGINT NOT NULL REFERENCES CURATED.DIM_AGENCY(agency_key),
  mission_type_key BIGINT NOT NULL REFERENCES CURATED.DIM_MISSION_TYPE(mission_type_key),
  orbit_key BIGINT NOT NULL REFERENCES CURATED.DIM_ORBIT(orbit_key),
  date_key BIGINT NOT NULL REFERENCES CURATED.DIM_DATE(date_key),
  payload_mass_kg DECIMAL(18, 3),
  launch_status VARCHAR,
  success_flag BOOLEAN,
  source_hash VARCHAR NOT NULL,
  source_loaded_at TIMESTAMPTZ,
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
  UNIQUE (source_name, source_id)
);
CREATE TABLE IF NOT EXISTS OPS.ETL_AUDIT (
  audit_key BIGINT PRIMARY KEY,
  run_id VARCHAR NOT NULL,
  pipeline_name VARCHAR NOT NULL,
  source_object VARCHAR NOT NULL,
  status VARCHAR NOT NULL,
  source_rows BIGINT NOT NULL DEFAULT 0,
  target_rows BIGINT,
  started_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ,
  error_message VARCHAR
);

INSERT INTO CURATED.DIM_ROCKET
  (rocket_key, rocket_name, family_name, attribute_hash, effective_from, is_current)
VALUES (0, 'Unknown', 'Unknown', 'UNKNOWN', TIMESTAMPTZ '1900-01-01 00:00:00+00', TRUE)
ON CONFLICT (rocket_key) DO NOTHING;
INSERT INTO CURATED.DIM_LAUNCH_SITE
  (launch_site_key, site_name, attribute_hash, effective_from, is_current)
VALUES (0, 'Unknown', 'UNKNOWN', TIMESTAMPTZ '1900-01-01 00:00:00+00', TRUE)
ON CONFLICT (launch_site_key) DO NOTHING;
INSERT INTO CURATED.DIM_AGENCY
  (agency_key, agency_name, attribute_hash, effective_from, is_current)
VALUES (0, 'Unknown', 'UNKNOWN', TIMESTAMPTZ '1900-01-01 00:00:00+00', TRUE)
ON CONFLICT (agency_key) DO NOTHING;
INSERT INTO CURATED.DIM_MISSION_TYPE
  (mission_type_key, mission_type_name, attribute_hash, effective_from, is_current)
VALUES (0, 'Unknown', 'UNKNOWN', TIMESTAMPTZ '1900-01-01 00:00:00+00', TRUE)
ON CONFLICT (mission_type_key) DO NOTHING;
INSERT INTO CURATED.DIM_ORBIT
  (orbit_key, orbit_name, attribute_hash, effective_from, is_current)
VALUES (0, 'Unknown', 'UNKNOWN', TIMESTAMPTZ '1900-01-01 00:00:00+00', TRUE)
ON CONFLICT (orbit_key) DO NOTHING;
INSERT INTO CURATED.DIM_DATE
  (date_key, full_date, calendar_year, calendar_quarter, month_number, month_name,
   day_of_month, day_of_week, is_weekend)
VALUES (0, DATE '1900-01-01', 1900, 1, 1, 'Unknown', 1, 1, FALSE)
ON CONFLICT (date_key) DO NOTHING;

CREATE OR REPLACE VIEW CURATED.VW_LAUNCH_CLEAN AS
WITH normalized AS (
  SELECT
    run_id,
    source_name,
    COALESCE(NULLIF(TRIM(source_id), ''), SHA256(CONCAT_WS('|',
      COALESCE(TRIM(launch_datetime), ''), COALESCE(TRIM(mission_name), ''),
      COALESCE(TRIM(rocket_name), ''), COALESCE(TRIM(company), '')
    ))) AS source_id,
    COALESCE(NULLIF(TRIM(mission_name), ''), 'Unknown') AS launch_name,
    TRY_CAST(launch_datetime AS TIMESTAMP) AS launch_timestamp,
    COALESCE(NULLIF(TRIM(rocket_name), ''), 'Unknown') AS rocket_name,
    COALESCE(NULLIF(TRIM(location), ''), 'Unknown') AS site_name,
    COALESCE(NULLIF(TRIM(agency), ''), NULLIF(TRIM(company), ''), 'Unknown') AS agency_name,
    COALESCE(NULLIF(TRIM(mission_type), ''), 'Unknown') AS mission_type_name,
    COALESCE(NULLIF(TRIM(orbit), ''), 'Unknown') AS orbit_name,
    TRY_CAST(REGEXP_REPLACE(payload_mass_kg, '[^0-9.-]', '', 'g') AS DECIMAL(18, 3)) AS payload_mass_kg,
    COALESCE(NULLIF(UPPER(TRIM(mission_status)), ''), 'UNKNOWN') AS launch_status,
    CASE
      WHEN REGEXP_MATCHES(UPPER(COALESCE(mission_status, '')), 'SUCCESS|SUCCESSFUL') THEN TRUE
      WHEN REGEXP_MATCHES(UPPER(COALESCE(mission_status, '')), 'FAIL|FAILURE') THEN FALSE
      ELSE NULL
    END AS success_flag,
    loaded_at AS source_loaded_at
  FROM RAW.LAUNCHES
),
deduplicated AS (
  SELECT *,
    CAST(launch_timestamp AS DATE) AS launch_date,
    ROW_NUMBER() OVER (
      PARTITION BY source_name, source_id
      ORDER BY source_loaded_at DESC, run_id DESC
    ) AS row_number
  FROM normalized
)
SELECT * EXCLUDE (row_number)
FROM deduplicated
WHERE row_number = 1;
