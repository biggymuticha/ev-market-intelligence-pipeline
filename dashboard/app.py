# dashboard/app.py
import os
import time
import boto3
import pandas as pd
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv

load_dotenv()

BUCKET    = os.getenv("S3_BUCKET_NAME")
REGION    = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
WORKGROUP = "ev_pipeline"
DATABASE  = "ev_pipeline"
RESULTS_S3 = f"s3://{BUCKET}/athena-results/"

st.set_page_config(page_title="EV Market Intelligence", page_icon="⚡", layout="wide")
st.title("⚡ Global EV Market Intelligence")
st.caption("Pipeline: Bruin → S3 bronze (Parquet) → dbt → S3 gold (Parquet) → Athena → Streamlit")

# ── Athena query helper ───────────────────────────────────────────────────────
@st.cache_data(ttl=3600)  # cache results 1 hour to avoid repeated Athena charges
def query_athena(sql: str) -> pd.DataFrame:
    client = boto3.client("athena", region_name=REGION)

    # Submit query
    response = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": RESULTS_S3},
        WorkGroup=WORKGROUP
    )
    query_id = response["QueryExecutionId"]

    # Poll until complete
    for _ in range(60):
        status = client.get_query_execution(QueryExecutionId=query_id)
        state  = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise RuntimeError(f"Athena query failed: {reason}")
        time.sleep(2)

    # Fetch results
    results = client.get_query_results(QueryExecutionId=query_id)
    columns = [col["Label"] for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = [
        [field.get("VarCharValue", "") for field in row["Data"]]
        for row in results["ResultSet"]["Rows"][1:]  # skip header row
    ]
    return pd.DataFrame(rows, columns=columns)

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.header("Filters")

years_df = query_athena(
    "SELECT DISTINCT year FROM fct_global_market_share ORDER BY year"
)
years = sorted(years_df["year"].astype(int).tolist())
selected_year = st.sidebar.selectbox(
    "Year (for market share tile)",
    years,
    index=len(years) - 1   # default to most recent year
)

powertrain_df = query_athena(
    "SELECT DISTINCT powertrain FROM fct_global_market_share ORDER BY powertrain"
)
powertrain_options = ["All"] + powertrain_df["powertrain"].tolist()
selected_powertrain = st.sidebar.selectbox("Powertrain", powertrain_options)

# Define this right after selected_powertrain is set in the sidebar
powertrain_filter = (
    ""
    if selected_powertrain == "All"
    else f"AND powertrain = '{selected_powertrain}'"
)

# ── Tile 1: Top regions by EV sales share (categorical bar chart) ─────────────
st.header("1. EV Sales Share by Region")
st.caption(f"EV sales as a percentage of all new car sales in {selected_year} — BEV only, excluding global aggregates")

df1 = query_athena(f"""
    SELECT
        region,
        CAST(avg_sales_share_pct AS double)  AS sales_share_pct,
        CAST(total_ev_sales AS double)        AS total_ev_sales
    FROM fct_global_market_share
    WHERE CAST(year AS int) = {selected_year}
      AND powertrain = 'BEV'
      AND region NOT IN ('World', 'Rest of the world', 'Other')
    ORDER BY sales_share_pct DESC
    LIMIT 15
""")

df1["sales_share_pct"] = pd.to_numeric(df1["sales_share_pct"])
df1["total_ev_sales"]  = pd.to_numeric(df1["total_ev_sales"])

fig1 = px.bar(
    df1,
    x="sales_share_pct",
    y="region",
    orientation="h",
    color="region",           # each region gets a distinct color
    text=df1["sales_share_pct"].apply(lambda x: f"{x:.1f}%"),
    labels={
        "sales_share_pct": "EV Sales Share (%)",
        "region": "Region"
    },
    title=f"Top 15 Regions — BEV Sales Share (%) in {selected_year}"
)
fig1.update_traces(textposition="outside")
fig1.update_layout(
    yaxis={"categoryorder": "total ascending"},
    showlegend=False,
    xaxis_title="EV Sales Share (%)",
    yaxis_title=""
)
st.plotly_chart(fig1, use_container_width=True)

# ── Graph 2: EV adoption trends over time (temporal line chart) ────────────────
st.header("2. EV Adoption Trends Over Time")
st.caption("Total EV units sold per year by region")

df2 = query_athena(f"""
    SELECT
        CAST(year AS int)              AS year,
        region,
        powertrain,
        CAST(total_ev_sales AS double) AS total_ev_sales,
        CAST(avg_yoy_growth_pct AS double) AS avg_yoy_growth_pct
    FROM fct_ev_adoption_trends
    {f"WHERE powertrain = '{selected_powertrain}'" if selected_powertrain != "All" else ""}
    ORDER BY year, region
""")

df2["year"]             = pd.to_numeric(df2["year"])
df2["total_ev_sales"]   = pd.to_numeric(df2["total_ev_sales"])
df2["avg_yoy_growth_pct"] = pd.to_numeric(df2["avg_yoy_growth_pct"])

fig2 = px.line(
    df2,
    x="year",
    y="total_ev_sales",
    color="region",
    line_dash="powertrain",   # different dash per powertrain (BEV vs PHEV)
    markers=True,
    hover_data=["avg_yoy_growth_pct"],
    labels={
        "year":           "Year",
        "total_ev_sales": "Total EV Units Sold",
        "region":         "Region",
        "avg_yoy_growth_pct": "YoY Growth (%)"
    },
    title="Global EV Adoption — Units Sold by Region (2011–2024)"
)
st.plotly_chart(fig2, use_container_width=True)

# ── Summary metrics row ───────────────────────────────────────────────────────
st.header("Summary")
summary = query_athena(f"""
    SELECT
        SUM(CAST(total_ev_sales AS double))        AS total_sales,
        AVG(CAST(avg_sales_share_pct AS double))   AS avg_share,
        AVG(CAST(avg_yoy_growth_pct AS double))    AS avg_growth
    FROM fct_ev_adoption_trends
    WHERE CAST(year AS int) = {selected_year}
    {powertrain_filter}
""")

col1, col2, col3 = st.columns(3)
col1.metric("Total EV Sales",      f"{int(float(summary['total_sales'][0])):,}")
col2.metric("Avg Sales Share",     f"{float(summary['avg_share'][0]):.2f}%")
col3.metric("Avg YoY Growth",      f"{float(summary['avg_growth'][0]):.2f}%")

st.markdown("---")
st.caption("Data: Global Electric Vehicle Sales Data (2010-2024) | Warehouse: Amazon Athena | Pipeline: Bruin + dbt")