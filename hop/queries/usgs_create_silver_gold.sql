/*
  USGS ELT target tables
  Jalankan file ini sekali di DBeaver menggunakan koneksi Supabase.
*/

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS silver.usgs_earthquakes (
    feature_type              TEXT,
    event_id                  TEXT PRIMARY KEY,
    magnitude                 NUMERIC(5, 2),
    place                     TEXT,
    event_time                TIMESTAMPTZ,
    updated_at                TIMESTAMPTZ,
    timezone_offset_minutes   INTEGER,
    event_url                 TEXT,
    detail_url                TEXT,
    felt_reports              INTEGER,
    cdi                       NUMERIC(6, 3),
    mmi                       NUMERIC(6, 3),
    alert                     TEXT,
    status                    TEXT,
    tsunami                   SMALLINT,
    significance              INTEGER,
    network                   TEXT,
    code                      TEXT,
    related_event_ids         TEXT,
    sources                   TEXT,
    product_types             TEXT,
    station_count             INTEGER,
    minimum_distance          NUMERIC(14, 6),
    rms_travel_time           NUMERIC(14, 6),
    azimuthal_gap             NUMERIC(14, 6),
    magnitude_type            TEXT,
    event_type                TEXT,
    title                     TEXT,
    geometry_type             TEXT,
    longitude                 NUMERIC(10, 6),
    latitude                  NUMERIC(10, 6),
    depth_km                  NUMERIC(12, 3),
    ingest_at                 TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_usgs_silver_event_time
    ON silver.usgs_earthquakes (event_time);

CREATE TABLE IF NOT EXISTS gold.usgs_earthquake_daily (
    event_date                DATE PRIMARY KEY,
    earthquake_count          INTEGER NOT NULL,
    avg_magnitude             NUMERIC(5, 2),
    max_magnitude             NUMERIC(5, 2),
    avg_depth_km               NUMERIC(12, 3),
    min_depth_km               NUMERIC(12, 3),
    magnitude_5_plus_count     INTEGER NOT NULL,
    tsunami_flag_count        INTEGER NOT NULL,
    processed_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);
