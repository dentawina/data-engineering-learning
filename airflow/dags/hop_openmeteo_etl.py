"""Run the repository's Apache Hop Open-Meteo pipeline from Airflow."""

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


HOP_ENV = {
    "HOP_HOME": "/opt/hop",
    "HOP_CONFIG_FOLDER": "/opt/airflow/.hop",
    "HOP_AUDIT_FOLDER": "/opt/airflow/logs/hop-audit",
}

SILVER_PIPELINE = "/opt/airflow/hop-project/pipelines/practice_hop.hpl"
GOLD_PIPELINE = "/opt/airflow/hop-project/pipelines/weather_gold.hpl"
REQUIRED_DATABASE_ENV = (
    "SUPABASE_DB_HOST",
    "SUPABASE_DB_PORT",
    "SUPABASE_DB_NAME",
    "SUPABASE_DB_USER",
    "SUPABASE_DB_PASSWORD",
)


with DAG(
    dag_id="hop_openmeteo_etl",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Jakarta"),
    schedule="10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["apache-hop", "open-meteo", "portfolio"],
) as dag:
    check_hop = BashOperator(
        task_id="check_hop",
        bash_command="""
set -euo pipefail
if ! command -v java >/dev/null 2>&1; then
    echo "Java tidak tersedia di container Airflow."
    exit 1
fi
if [ ! -x /opt/hop/hop-run.sh ]; then
    echo "Apache Hop runner tidak executable: /opt/hop/hop-run.sh"
    exit 1
fi
java -version
version_output=""
version_status=0
set +e
version_output=$(/opt/hop/hop-run.sh --version 2>&1)
version_status=$?
set -e
printf '%s\n' "$version_output"
if ! printf '%s\n' "$version_output" | grep -q '2[.]19[.]0'; then
    echo "Apache Hop tidak dapat diverifikasi sebagai versi 2.19.0."
    exit 1
fi
if [ "$version_status" -ne 0 ]; then
    echo "Peringatan: hop-run --version mengembalikan status $version_status setelah mencetak versi."
fi
""",
        env=HOP_ENV,
        append_env=True,
    )

    check_pipeline = BashOperator(
        task_id="check_pipeline",
        bash_command="""
set -euo pipefail
if [ -f /opt/airflow/.env ]; then
    set -a
    . /opt/airflow/.env
    set +a
fi
for pipeline in ${PIPELINES}; do
    if [ ! -f "$pipeline" ]; then
        echo "Pipeline tidak ditemukan: $pipeline"
        exit 1
    fi
done
for variable in ${REQUIRED_DATABASE_ENV}; do
    if [ -z "${!variable:-}" ]; then
        echo "Environment variable database wajib belum tersedia: $variable"
        exit 1
    fi
done
echo "Pipeline silver dan gold tersedia; environment database wajib terisi."
""",
        env={
            **HOP_ENV,
            "PIPELINES": f"{SILVER_PIPELINE} {GOLD_PIPELINE}",
            "REQUIRED_DATABASE_ENV": " ".join(REQUIRED_DATABASE_ENV),
        },
        append_env=True,
    )

    run_silver_pipeline = BashOperator(
        task_id="run_silver_pipeline",
        bash_command="""
set -euo pipefail
if [ -f /opt/airflow/.env ]; then
    set -a
    . /opt/airflow/.env
    set +a
fi
mkdir -p "$HOP_AUDIT_FOLDER"
/opt/hop/hop-run.sh \
    --project=default \
    --runconfig=local \
    --system-properties="SUPABASE_DB_HOST=$SUPABASE_DB_HOST" \
    --system-properties="SUPABASE_DB_PORT=$SUPABASE_DB_PORT" \
    --system-properties="SUPABASE_DB_NAME=$SUPABASE_DB_NAME" \
    --system-properties="SUPABASE_DB_USER=$SUPABASE_DB_USER" \
    --system-properties="SUPABASE_DB_PASSWORD=$SUPABASE_DB_PASSWORD" \
    --file="$PIPELINE" \
    --level=Basic
""",
        env={**HOP_ENV, "PIPELINE": SILVER_PIPELINE},
        append_env=True,
    )

    run_gold_pipeline = BashOperator(
        task_id="run_gold_pipeline",
        bash_command="""
set -euo pipefail
if [ -f /opt/airflow/.env ]; then
    set -a
    . /opt/airflow/.env
    set +a
fi
mkdir -p "$HOP_AUDIT_FOLDER"
/opt/hop/hop-run.sh \
    --project=default \
    --runconfig=local \
    --system-properties="SUPABASE_DB_HOST=$SUPABASE_DB_HOST" \
    --system-properties="SUPABASE_DB_PORT=$SUPABASE_DB_PORT" \
    --system-properties="SUPABASE_DB_NAME=$SUPABASE_DB_NAME" \
    --system-properties="SUPABASE_DB_USER=$SUPABASE_DB_USER" \
    --system-properties="SUPABASE_DB_PASSWORD=$SUPABASE_DB_PASSWORD" \
    --file="$PIPELINE" \
    --level=Basic
""",
        env={**HOP_ENV, "PIPELINE": GOLD_PIPELINE},
        append_env=True,
    )

    check_hop >> check_pipeline >> run_silver_pipeline >> run_gold_pipeline
