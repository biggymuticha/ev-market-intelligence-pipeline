""" @bruin
name: gold.dbt_models
type: python
connection: duckdb-default
description: "Runs dbt transformation models after ingestion."
depends:
  - bronze.ev_sales_parquet
@bruin """

# pipelines/ev_ingestion/assets/run_dbt.py
# Bruin runs this asset AFTER ingest_to_s3.py because of the "depends" field above.

import subprocess
import sys

print("Running dbt models...")
result = subprocess.run(
    ["dbt", "run", "--project-dir", "transform", "--profiles-dir", "transform"],
    capture_output=True, text=True
)
print(result.stdout)
if result.returncode != 0:
    print(result.stderr)
    sys.exit(1)
print("dbt COMPLETE." )
