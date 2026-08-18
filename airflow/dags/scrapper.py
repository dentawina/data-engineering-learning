import re
import pandas as pd
import pendulum
from google_play_scraper import reviews, Sort
from airflow.sdk import dag, task, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import date


APP_ID = "com.dafturn.mypertamina"

POSTGRES_CONN_ID = "supabase_postgres"

SCHEMA_NAME = "public"
TABLE_NAME = "scrapper_mypertamina"

LOCAL_TZ = pendulum.timezone("Asia/Jakarta")


@dag(
    dag_id="ingest_mypertamina_reviews",
    schedule="10 0 * * *",  # setiap hari jam 00:10 WIB
    start_date=pendulum.datetime(
        2026,
        8,
        18,
        tz=LOCAL_TZ,
    ),
    catchup=False,
    max_active_runs=1,
    tags=[
        "google-play",
        "mypertamina",
        "ingestion",
    ],
)
def ingest_mypertamina_reviews():

    @task(
        retries=2,
        retry_delay=pendulum.duration(minutes=5),
    )
    def extract_reviews():
        """
        Scrape review terbaru Google Play
        dan ambil hanya data untuk tanggal
        interval Airflow yang sedang diproses.
        """

        context = get_current_context()

        #target_date = (context["data_interval_start"].in_timezone(LOCAL_TZ).date())
        target_date = date(2026, 8, 17)

        print("=" * 50)
        print(f"Target date : {target_date}")
        print(f"App ID      : {APP_ID}")
        print("=" * 50)

        result, continuation_token = reviews(
            APP_ID,
            lang="id",
            country="id",
            sort=Sort.NEWEST,
            count=1000,
        )

        df = pd.DataFrame(result)

        print(f"Total scraped: {len(df)}")

        if df.empty:
            print("Scraper tidak mengembalikan data.")
            return []

        # camelCase -> snake_case
        df.columns = [
            re.sub(
                r"(?<!^)(?=[A-Z])",
                "_",
                col,
            ).lower()
            for col in df.columns
        ]

        # Rename agar lebih jelas
        df = df.rename(
            columns={
                "at": "review_created_at",
                "replied_at": "developer_replied_at",
            }
        )

        # pastikan datetime
        df["review_created_at"] = pd.to_datetime(
            df["review_created_at"],
            errors="coerce",
        )

        if "developer_replied_at" in df.columns:
            df["developer_replied_at"] = pd.to_datetime(
                df["developer_replied_at"],
                errors="coerce",
            )

        # filter sesuai tanggal yang diproses Airflow
        df = df[
            df["review_created_at"].dt.date
            == target_date
        ].copy()

        print(
            f"Review ditemukan untuk "
            f"{target_date}: {len(df)}"
        )

        if df.empty:
            return []

        # conversion agar aman masuk XCom JSON
        datetime_columns = [
            "review_created_at",
            "developer_replied_at",
        ]

        for col in datetime_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: (
                        x.isoformat()
                        if pd.notnull(x)
                        else None
                    )
                )

        return df.to_dict(
            orient="records"
        )

    @task(
        retries=2,
        retry_delay=pendulum.duration(minutes=5),
    )
    def load_to_supabase(rows):
        """
        Load review ke Supabase PostgreSQL
        menggunakan Airflow Connection.
        """

        if not rows:
            print(
                "Tidak ada data yang perlu "
                "dimasukkan ke PostgreSQL."
            )
            return {
                "inserted_rows": 0
            }

        df = pd.DataFrame(rows)

        # kembalikan menjadi datetime
        if "review_created_at" in df.columns:
            df["review_created_at"] = pd.to_datetime(
                df["review_created_at"]
            )

        if "developer_replied_at" in df.columns:
            df["developer_replied_at"] = pd.to_datetime(
                df["developer_replied_at"]
            )

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        engine = hook.get_sqlalchemy_engine()

        df.to_sql(
            name=TABLE_NAME,
            con=engine,
            schema=SCHEMA_NAME,
            if_exists="append",
            index=False,
            method="multi",
        )

        inserted_rows = len(df)

        print("=" * 50)
        print(
            f"{inserted_rows} rows berhasil "
            "dimasukkan ke Supabase"
        )
        print(
            f"Table: "
            f"{SCHEMA_NAME}.{TABLE_NAME}"
        )
        print("=" * 50)

        return {
            "inserted_rows": inserted_rows
        }
    rows = extract_reviews()
    load_to_supabase(rows)
ingest_mypertamina_reviews()