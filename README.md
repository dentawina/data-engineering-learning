# Data Engineering Portfolio

Repository ini menyimpan DAG Airflow dan pipeline Apache Hop yang dapat dijalankan
di Windows (Hop GUI) maupun container Linux.

## Struktur Apache Hop

```text
hop/
├── project-config.json
├── environments/
│   └── supabase_denta.environment.json
└── pipelines/
    └── practice_hop.hpl
```

`project-config.json` berasal dari konfigurasi Hop dan menggunakan `${PROJECT_HOME}`
sehingga tidak bergantung pada path Windows. File environment yang di-commit hanya
memetakan lima variabel Hop ke environment variable OS; nilainya tidak menyimpan
kredensial.

Folder runtime/config seperti `hop/config/` dan `.hop/` di-ignore agar kredensial
dan metadata lokal tidak masuk Git. Metadata koneksi Hop bernama `supabase_denta`
belum tersedia di source yang diberikan; metadata tersebut harus tersedia pada
`HOP_CONFIG_FOLDER` di Windows dan worker Airflow sebelum pipeline dijalankan.

## Environment database

Salin `.env.example` menjadi file lokal lalu isi lima variabel `SUPABASE_DB_*`.
Jangan commit file hasil salinan tersebut. Di container Airflow, variabel itu harus
tersedia pada worker Celery yang menjalankan task.

Jika environment variable tidak diberikan langsung oleh Docker Compose, mount
file `.env` lokal secara read-only ke `/opt/airflow/.env` pada service worker.
Jangan menyalin file itu ke Git. Contoh konfigurasi service worker:

```yaml
env_file:
  - .env
```

DAG juga akan membaca `/opt/airflow/.env` bila file tersebut di-mount. File
`supabase_denta.env` lama yang hanya berisi `POSTGRES_URL` tidak cukup untuk DAG
ini; worker harus menerima lima variable `SUPABASE_DB_*`.

## Hop GUI di Windows

1. Buka project repository dengan Apache Hop GUI, lalu gunakan
   `hop/project-config.json` sebagai project configuration file dan buka
   `hop/pipelines/practice_hop.hpl`.
2. Pastikan environment variable `SUPABASE_DB_HOST`, `SUPABASE_DB_PORT`,
   `SUPABASE_DB_NAME`, `SUPABASE_DB_USER`, dan `SUPABASE_DB_PASSWORD` tersedia
   pada proses Hop GUI.
3. Simpan perubahan kembali ke file yang sama. Commit pipeline dan project config
   yang sudah diperiksa; jangan commit `hop/config/`, `.hop/`, atau file environment
   lokal yang berisi nilai asli.

## Menjalankan manual di container

Dengan mount project ke `/opt/airflow/hop-project` dan konfigurasi Hop ke
`/opt/airflow/.hop`, jalankan:

```bash
/opt/hop/hop-run.sh \
  --file="/opt/airflow/hop-project/pipelines/practice_hop.hpl" \
  --level=Basic
```

Sebelum menjalankan, pastikan Java, `hop-run.sh`, pipeline, metadata koneksi
`supabase_denta`, dan lima environment variable database tersedia. DAG
`hop_openmeteo_etl` melakukan pemeriksaan dasar yang sama secara otomatis.

## Validasi DAG

```bash
airflow dags list | grep hop_openmeteo_etl
airflow dags show hop_openmeteo_etl
python -m py_compile airflow/dags/hop_openmeteo_etl.py
```

Jadwal DAG adalah `10 * * * *` dengan timezone `Asia/Jakarta`, tanpa catchup, dan
maksimal satu run aktif.

## Alur Windows ke server

Di Windows, setelah menguji dan meninjau perubahan:

```bash
git add hop/pipelines/practice_hop.hpl airflow/dags/hop_openmeteo_etl.py README.md .gitignore .env.example
git commit -m "Add Apache Hop Airflow ETL integration"
git push
```

Di server, lakukan pull lalu validasi dan uji manual:

```bash
git pull
python -m py_compile airflow/dags/hop_openmeteo_etl.py
/opt/hop/hop-run.sh --file="/opt/airflow/hop-project/pipelines/practice_hop.hpl" --level=Basic
airflow dags list | grep hop_openmeteo_etl
```

Perintah Git push/pull dan deployment harus dijalankan oleh pemilik environment
setelah meninjau perubahan lokal.
