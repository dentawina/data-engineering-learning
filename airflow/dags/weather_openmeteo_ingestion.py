import logging
from datetime import timedelta

import pandas as pd
import pendulum
import requests

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ============================================================
# Configuration
# ============================================================

DAG_ID = "weather_openmeteo_ingestion"

POSTGRES_CONN_ID = "supabase_postgres"

TARGET_SCHEMA = "public"
TARGET_TABLE = "scrapper_openmeteo"

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

TIMEZONE = "Asia/Jakarta"

YOGYAKARTA = {
    "city": "Yogyakarta",
    "province": "DI Yogyakarta",
    "latitude": -7.8,
    "longitude": 110.4,
}


logger = logging.getLogger(__name__)


# ============================================================
# DAG definition
# ============================================================

@dag(
    dag_id=DAG_ID,

    # Berjalan setiap jam pada menit ke-5
    schedule="5 * * * *",

    start_date=pendulum.datetime(
        2026,
        8,
        19,
        tz=TIMEZONE,
    ),

    catchup=False,

    # Mencegah dua DAG run aktif bersamaan
    max_active_runs=1,

    tags=[
        "weather",
        "open-meteo",
        "postgres",
        "learning",
    ],
)
def weather_openmeteo_pipeline():

    @task(
        task_id="ingest_yogyakarta_weather",
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=10),
    )
    def ingest_weather():

        logger.info(
            "Starting Open-Meteo ingestion | city=%s",
            YOGYAKARTA["city"],
        )

        # ====================================================
        # 1. Extract from API
        # ====================================================

        params = {
            "latitude": YOGYAKARTA["latitude"],
            "longitude": YOGYAKARTA["longitude"],
            "hourly": (
                "temperature_2m,"
                "relative_humidity_2m,"
                "apparent_temperature,"
                "precipitation,"
                "rain,"
                "weather_code,"
                "cloud_cover,"
                "surface_pressure,"
                "wind_speed_10m,"
                "wind_direction_10m"
            ),

            # Ambil satu jam terakhir
            "past_hours": 1,

            # Tidak mengambil data forecast ke depan
            "forecast_hours": 0,

            "timezone": TIMEZONE,
        }

        logger.info(
            "Requesting Open-Meteo API | city=%s",
            YOGYAKARTA["city"],
        )

        response = requests.get(
            OPEN_METEO_URL,
            params=params,
            timeout=30,
        )

        logger.info(
            "Open-Meteo response received | city=%s | "
            "http_status=%s",
            YOGYAKARTA["city"],
            response.status_code,
        )

        response.raise_for_status()

        data = response.json()

        # ====================================================
        # 2. Transform to DataFrame
        # ====================================================

        if "hourly" not in data:
            raise ValueError(
                "Key 'hourly' tidak ditemukan dalam response "
                "Open-Meteo"
            )

        df_api = pd.DataFrame(data["hourly"])

        if df_api.empty:
            raise ValueError(
                "Open-Meteo mengembalikan DataFrame kosong"
            )

        # Sebagai pengaman, hanya gunakan timestamp terakhir
        df_api = df_api.tail(1).copy()

        df_api = df_api.rename(
            columns={
                "time": "observed_at",
            }
        )

        # Tambahkan metadata kota
        df_api["city"] = YOGYAKARTA["city"]
        df_api["province"] = YOGYAKARTA["province"]

        df_api["latitude"] = data["latitude"]
        df_api["longitude"] = data["longitude"]
        df_api["timezone"] = data["timezone"]

        df_api["source"] = "open_meteo"

        df_api["ingest_at"] = pd.Timestamp.now(
            tz=TIMEZONE
        )

        # Timestamp dari API berbentuk local time
        df_api["observed_at"] = (
            pd.to_datetime(df_api["observed_at"])
            .dt.tz_localize(TIMEZONE)
        )

        # Susun urutan kolom
        df_api = df_api[
            [
                "city",
                "province",
                "latitude",
                "longitude",
                "timezone",
                "observed_at",
                "temperature_2m",
                "relative_humidity_2m",
                "apparent_temperature",
                "precipitation",
                "rain",
                "weather_code",
                "cloud_cover",
                "surface_pressure",
                "wind_speed_10m",
                "wind_direction_10m",
                "source",
                "ingest_at",
            ]
        ]

        logger.info(
            "Weather DataFrame prepared | city=%s | "
            "observed_at=%s | row_count=%s | ingest_at=%s",
            YOGYAKARTA["city"],
            df_api["observed_at"].iloc[0],
            len(df_api),
            df_api["ingest_at"].iloc[0],
        )

        # ====================================================
        # 3. Load to Supabase PostgreSQL
        # ====================================================

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        engine = hook.get_sqlalchemy_engine()

        try:
            df_api.to_sql(
                name=TARGET_TABLE,
                con=engine,
                schema=TARGET_SCHEMA,
                if_exists="append",
                index=False,
                method="multi",
            )

        finally:
            engine.dispose()

        logger.info(
            "Weather ingestion successful | "
            "target=%s.%s | city=%s | "
            "observed_at=%s | inserted_rows=%s",
            TARGET_SCHEMA,
            TARGET_TABLE,
            YOGYAKARTA["city"],
            df_api["observed_at"].iloc[0],
            len(df_api),
        )

    ingest_weather()


weather_openmeteo_pipeline()