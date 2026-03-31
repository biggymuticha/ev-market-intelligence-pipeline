# scripts/register_athena_tables.py
# Registers the dbt gold Parquet files in the Glue Data Catalog.
# Run this once after the first dbt run.
#
# Why is this needed?
#   Athena uses Glue as a schema registry. Without a table entry in Glue,
#   Athena doesn't know the file exists or what columns it has.

import boto3
import os
from dotenv import load_dotenv

load_dotenv()

BUCKET = os.getenv("S3_BUCKET_NAME")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
GLUE_DB = "ev_pipeline"

glue = boto3.client("glue", region_name=REGION)

TABLES = [
    {
        "name": "fct_global_market_share",
        "s3_path": f"s3://{BUCKET}/gold/fct_global_market_share/",  
        "columns": [
            {"Name": "year",                  "Type": "int"},
            {"Name": "region",                "Type": "string"},
            {"Name": "powertrain",            "Type": "string"},
            {"Name": "avg_sales_share_pct",   "Type": "double"},
            {"Name": "total_ev_sales",        "Type": "double"},
            {"Name": "avg_stock_share_pct",   "Type": "double"},
            {"Name": "total_charging_points", "Type": "double"},
            {"Name": "region_rank",           "Type": "bigint"},
        ]
    },
    {
        "name": "fct_ev_adoption_trends",
        "s3_path": f"s3://{BUCKET}/gold/fct_ev_adoption_trends/",   
        "columns": [
            {"Name": "year",               "Type": "int"},
            {"Name": "region",             "Type": "string"},
            {"Name": "powertrain",         "Type": "string"},
            {"Name": "total_ev_sales",     "Type": "double"},
            {"Name": "avg_sales_share_pct","Type": "double"},
            {"Name": "avg_yoy_growth_pct", "Type": "double"},
        ]
    },
]

for table in TABLES:
    # Delete existing table definition if it exists
    try:
        glue.delete_table(DatabaseName=GLUE_DB, Name=table["name"])
        print(f"Deleted existing: {table['name']}")
    except glue.exceptions.EntityNotFoundException:
        pass

    glue.create_table(
        DatabaseName=GLUE_DB,
        TableInput={
            "Name": table["name"],
            "StorageDescriptor": {
                "Columns": table["columns"],
                "Location": table["s3_path"],
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                },
            },
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "parquet"}
        }
    )
    print(f"Registered: {GLUE_DB}.{table['name']} → {table['s3_path']}")

print("Done. Tables are now queryable in Athena.")