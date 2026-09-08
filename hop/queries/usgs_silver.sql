/*
  Silver USGS earthquake data

  Grain: one latest valid record per event_id.
  Raw keeps every received version; Silver keeps the latest version.
*/

WITH cleaned AS (
    SELECT
        NULLIF(BTRIM(feature_type), '') AS feature_type,
        NULLIF(BTRIM(event_id), '') AS event_id,
        magnitude::numeric,
        NULLIF(BTRIM(place), '') AS place,
        event_time,
        updated_at,
        NULLIF(BTRIM(timezone_offset_minutes), '')::numeric::integer AS timezone_offset_minutes,
        NULLIF(BTRIM(event_url), '') AS event_url,
        NULLIF(BTRIM(detail_url), '') AS detail_url,
        felt_reports::integer,
        cdi::numeric,
        mmi::numeric,
        NULLIF(BTRIM(alert), '') AS alert,
        NULLIF(BTRIM(status), '') AS status,
        tsunami::smallint,
        significance::integer,
        NULLIF(BTRIM(network), '') AS network,
        NULLIF(BTRIM(code), '') AS code,
        NULLIF(BTRIM(related_event_ids), '') AS related_event_ids,
        NULLIF(BTRIM(sources), '') AS sources,
        NULLIF(BTRIM(product_types), '') AS product_types,
        station_count::integer,
        minimum_distance::numeric,
        rms_travel_time::numeric,
        azimuthal_gap::numeric,
        NULLIF(BTRIM(magnitude_type), '') AS magnitude_type,
        NULLIF(BTRIM(event_type), '') AS event_type,
        NULLIF(BTRIM(title), '') AS title,
        NULLIF(BTRIM(geometry_type), '') AS geometry_type,
        longitude::numeric,
        latitude::numeric,
        depth_km::numeric,
        ingest_at
    FROM public.usgs_earthquakes_raw
    WHERE NULLIF(BTRIM(event_id), '') IS NOT NULL
      AND event_time IS NOT NULL
      AND updated_at IS NOT NULL
      AND event_time <= updated_at
      AND longitude BETWEEN -180 AND 180
      AND latitude BETWEEN -90 AND 90
      AND depth_km BETWEEN -100 AND 1000
      AND magnitude BETWEEN -2 AND 10
      AND LOWER(NULLIF(BTRIM(event_type), '')) = 'earthquake'
),
ranked AS (
    SELECT
        cleaned.*,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY updated_at DESC, ingest_at DESC
        ) AS record_rank
    FROM cleaned
)
SELECT
    feature_type,
    event_id,
    magnitude,
    place,
    event_time,
    updated_at,
    timezone_offset_minutes,
    event_url,
    detail_url,
    felt_reports,
    cdi,
    mmi,
    alert,
    status,
    tsunami,
    significance,
    network,
    code,
    related_event_ids,
    sources,
    product_types,
    station_count,
    minimum_distance,
    rms_travel_time,
    azimuthal_gap,
    magnitude_type,
    event_type,
    title,
    geometry_type,
    longitude,
    latitude,
    depth_km,
    ingest_at
FROM ranked
WHERE record_rank = 1;
