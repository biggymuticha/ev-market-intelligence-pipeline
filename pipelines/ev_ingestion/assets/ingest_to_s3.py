""" @bruin
name: bronze.ev_sales_parquet
type: python
connection: duckdb-default
description: "Downloads EV sales CSV from Kaggle, converts to Parquet, uploads to S3 bronze layer."
@bruin """

# pipelines/ev_ingestion/assets/ingest_to_s3.py
#
# What this does:
#   1. Downloads the EV sales CSV from Kaggle
#   2. Uses DuckDB to convert it to Parquet (columnar format — faster + cheaper for Athena)
#   3. Uploads the Parquet file to s3://your-bucket/bronze/ev_sales/
#
# Why Parquet on S3?
#   Athena charges per TB scanned. Parquet is columnar and compressed — typically
#   10-20x smaller than CSV. When using parquet, estimate cost per query is $0.001 .

import os
import subprocess
import duckdb
import boto3
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
BUCKET = os.getenv("S3_BUCKET_NAME")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BRONZE_PREFIX = "bronze/ev_sales"
LOCAL_DIR = Path("data/raw")
LOCAL_DIR.mkdir(parents=True, exist_ok=True)

# ── Step 1: Download from Kaggle ──────────────────────────────────────────────
os.environ["KAGGLE_USERNAME"] = os.getenv("KAGGLE_USERNAME", "")
os.environ["KAGGLE_KEY"] = os.getenv("KAGGLE_KEY", "")

DATASET = "muhammadehsan000/global-electric-vehicle-sales-data-2010-2024"
print(f"Downloading {DATASET} ...")
subprocess.run(
    ["kaggle", "datasets", "download", "-d", DATASET, "-p", str(LOCAL_DIR), "--unzip"],
    check=True
)

# ── Step 2: Convert CSV → Parquet using DuckDB ────────────────────────────────
csv_files = list(LOCAL_DIR.glob("*.csv"))
if not csv_files:
    raise FileNotFoundError("No CSV files found after download")

parquet_path = LOCAL_DIR / "ev_sales.parquet"
print(f"Converting {len(csv_files)} CSV file(s) to Parquet...")

con = duckdb.connect()
con.execute(f"""
    COPY (
        SELECT * FROM read_csv_auto('{LOCAL_DIR}/*.csv', union_by_name=true)
    )
    TO '{parquet_path}'
    (FORMAT PARQUET, COMPRESSION ZSTD)
""")
# ZSTD compression: good balance of speed and size — reduces file ~5x vs uncompressed

file_size_mb = parquet_path.stat().st_size / 1024 / 1024
print(f"Parquet written: {parquet_path} ({file_size_mb:.2f} MB)")
con.close()

# ── Step 3: Upload Parquet to S3 bronze layer ─────────────────────────────────
s3 = boto3.client("s3", region_name=REGION)
s3_key = f"{BRONZE_PREFIX}/ev_sales.parquet"

print(f"Uploading to s3://{BUCKET}/{s3_key} ...")
s3.upload_file(str(parquet_path), BUCKET, s3_key)
print("Upload complete ✓")

# ── Step 4: Verify upload ─────────────────────────────────────────────────────
response = s3.head_object(Bucket=BUCKET, Key=s3_key)
size_mb = response["ContentLength"] / 1024 / 1024
print(f"Verified on S3: {size_mb:.2f} MB at s3://{BUCKET}/{s3_key}")
