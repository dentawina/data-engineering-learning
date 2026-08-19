import re
import pandas as pd
import pendulum
from google_play_scraper import reviews, Sort
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

APP_ID = "com.dafturn.mypertamina"
POSTGRES_CONN_ID = "supabase_postgres"
SCHEMA_NAME = "public"
TABLE_NAME = "scrapper_mypertamina"
LOCAL_TZ = pendulum.timezone("Asia/Jakarta")


@dag(
    dag_id="ingest_mypertamina_reviews",
    schedule="10 0 * * *",
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
    def ingest_reviews():

        # H-1 waktu Jakarta
        target_date = (
            pendulum.now("Asia/Jakarta")
            .subtract(days=1)
            .date()
        )

        print(f"Target date: {target_date}")

        # Extract
        result, _ = reviews(
            APP_ID,
            lang="id",
            country="id",
            sort=Sort.NEWEST,
            count=1000,
        )

        df = pd.DataFrame(result)

        print(f"Total scraped: {len(df)}")

        if df.empty:
            print("Tidak ada data dari Google Play.")
            return

        # camelCase -> snake_case
        df.columns = [
            re.sub(
                r"(?<!^)(?=[A-Z])",
                "_",
                col,
            ).lower()
            for col in df.columns
        ]

        # Rename
        df = df.rename(
            columns={
                "at": "review_created_at",
                "replied_at": "developer_replied_at",
            }
        )

        # Datetime
        df["review_created_at"] = pd.to_datetime(df["review_created_at"],errors="coerce",)
        df["developer_replied_at"] = pd.to_datetime(df["developer_replied_at"],errors="coerce",)

        # Filter H-1
        df = df[df["review_created_at"].dt.date== target_date].copy()
        print(f"Review tanggal {target_date}: {len(df)}")
        if df.empty:
            print("Tidak ada review yang perlu diinsert.")
            return

        # Ingestion timestamp
        ingest_time = pendulum.now("Asia/Jakarta")
        df["ingest_at"] = ingest_time
        # Connection Airflow
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()
        # Load
        df.to_sql(
            name=TABLE_NAME,
            con=engine,
            schema=SCHEMA_NAME,
            if_exists="append",
            index=False,
            method="multi",)

        print("=" * 50)
        print(f"Inserted rows : {len(df)}")
        print(f"Target date   : {target_date}")
        print(f"Table         : {SCHEMA_NAME}.{TABLE_NAME}")
        print("=" * 50)

    ingest_reviews()


ingest_mypertamina_reviews()