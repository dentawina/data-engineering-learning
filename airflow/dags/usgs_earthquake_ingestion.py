"""Ingest yesterday's USGS earthquake events into PostgreSQL."""

import logging
from datetime import timedelta

import pandas as pd
import pendulum
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task


logger = logging.getLogger(__name__)

TIMEZONE = "Asia/Jakarta"
POSTGRES_CONN_ID = "supabase_postgres"
TARGET_SCHEMA = "public"
TARGET_TABLE = "usgs_earthquakes_raw"
USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"


def _utc_iso(value):
    """Return an ISO-8601 UTC timestamp accepted by the USGS API."""

    return value.in_timezone("UTC").to_iso8601_string()


def _request_features(params):
    response = requests.get(USGS_URL, params=params, timeout=120)
    response.raise_for_status()
    payload = response.json()

    if payload.get("type") != "FeatureCollection":
        raise ValueError("Respons USGS bukan GeoJSON FeatureCollection")

    features = payload.get("features")
    if not isinstance(features, list):
        raise ValueError("Key features dari respons USGS bukan list")

    result_count = payload.get("metadata", {}).get("count")
    if result_count and result_count > params.get("limit", 20_000):
        raise ValueError(
            "Respons USGS melebihi limit. Perkecil window query sebelum load."
        )

    return features


def _flatten_features(features):
    """Flatten GeoJSON properties for the existing public table schema."""

    frame = pd.json_normalize(features, sep="_")
    if frame.empty:
        return frame

    coordinates = frame["geometry_coordinates"].apply(pd.Series)
    frame["longitude"] = coordinates[0]
    frame["latitude"] = coordinates[1]
    frame["depth_km"] = coordinates[2]
    frame = frame.drop(columns=["geometry_coordinates"])

    frame = frame.rename(
        columns={
            "type": "feature_type",
            "id": "event_id",
            "properties_mag": "magnitude",
            "properties_magType": "magnitude_type",
            "properties_place": "place",
            "properties_time": "event_time",
            "properties_updated": "updated_at",
            "properties_tz": "timezone_offset_minutes",
            "properties_url": "event_url",
            "properties_detail": "detail_url",
            "properties_felt": "felt_reports",
            "properties_cdi": "cdi",
            "properties_mmi": "mmi",
            "properties_alert": "alert",
            "properties_status": "status",
            "properties_tsunami": "tsunami",
            "properties_sig": "significance",
            "properties_net": "network",
            "properties_code": "code",
            "properties_ids": "related_event_ids",
            "properties_sources": "sources",
            "properties_types": "product_types",
            "properties_nst": "station_count",
            "properties_dmin": "minimum_distance",
            "properties_rms": "rms_travel_time",
            "properties_gap": "azimuthal_gap",
            "properties_type": "event_type",
            "properties_title": "title",
        }
    )

    frame["event_time"] = pd.to_datetime(
        frame["event_time"], unit="ms", utc=True
    )
    frame["updated_at"] = pd.to_datetime(
        frame["updated_at"], unit="ms", utc=True
    )
    frame["ingest_at"] = pd.Timestamp.now(tz="UTC")

    return frame


@dag(
    dag_id="usgs_earthquake_ingestion",
    schedule="30 0 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["usgs", "earthquake", "postgres", "elt"],
)
def usgs_earthquake_pipeline():

    @task(
        task_id="ingest_yesterday_usgs_events",
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=15),
    )
    def ingest_yesterday_events():
        target_day_wib = pendulum.now(TIMEZONE).subtract(days=1)
        target_date = target_day_wib.date()
        start_wib = pendulum.datetime(
            target_date.year,
            target_date.month,
            target_date.day,
            tz=TIMEZONE,
        )
        end_wib = start_wib.add(days=1).subtract(microseconds=1000)
        start_utc = _utc_iso(start_wib)
        end_utc = _utc_iso(end_wib)

        logger.info(
            "USGS window | target_date_wib=%s | start_utc=%s | end_utc=%s",
            target_date,
            start_utc,
            end_utc,
        )

        event_features = _request_features(
            {
                "format": "geojson",
                "starttime": start_utc,
                "endtime": end_utc,
                "eventtype": "earthquake",
                "orderby": "time-asc",
                "limit": 20_000,
            }
        )

        updated_features = _request_features(
            {
                "format": "geojson",
                "starttime": "1900-01-01T00:00:00Z",
                "endtime": end_utc,
                "updatedafter": start_utc,
                "eventtype": "earthquake",
                "orderby": "updated",
                "limit": 20_000,
            }
        )

        # updatedafter has no updated-before parameter. Keep only updates
        # whose updated_at date is actually the target date in WIB.
        updated_features = [
            feature
            for feature in updated_features
            if pendulum.from_timestamp(
                feature["properties"]["updated"] / 1000,
                tz="UTC",
            ).in_timezone(TIMEZONE).date()
            == target_date
        ]

        features = event_features + updated_features
        frame = _flatten_features(features)

        if frame.empty:
            logger.info(
                "No USGS events to append | target_date_wib=%s",
                target_date,
            )
            return

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()
        try:
            frame.to_sql(
                name=TARGET_TABLE,
                con=engine,
                schema=TARGET_SCHEMA,
                if_exists="append",
                index=False,
                chunksize=500,
                method="multi",
            )
        finally:
            engine.dispose()

        logger.info(
            "USGS ingestion successful | target=%s.%s | "
            "event_rows=%s | updated_rows=%s | appended_rows=%s",
            TARGET_SCHEMA,
            TARGET_TABLE,
            len(event_features),
            len(updated_features),
            len(frame),
        )

    ingest_yesterday_events()


usgs_earthquake_pipeline()
