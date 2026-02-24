#!/usr/bin/env python3
import os
import pathlib
from datetime import date

import boto3
import duckdb
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

OUT_DIR = pathlib.Path("out")
TMP_DIR = pathlib.Path("tmp")
OUT_DIR.mkdir(parents=True, exist_ok=True)
TMP_DIR.mkdir(parents=True, exist_ok=True)

SOURCES = [
    "accounting_entries_review_pending",
    "accounting_assets_pending",
    "banking_reconcile_pending",
    "banking_account_sync_error",
    "banking_account_sync_pending",
    "contact_contact_new",
    "contact_pending_review",
    "invoicing_expense_pending",
    "invoicing_inbox_new",
    "invoicing_invoice_pending",
    "invoicing_scan_pending",
    "invoicing_scan_accountant_consumed",
    "invoicing_purchase_count",
]


def require(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise SystemExit(f"Missing required env var: {name}")
    return v


def download_from_spaces(local_path: pathlib.Path) -> None:
    endpoint = require("SPACES_ENDPOINT")
    region = os.getenv("SPACES_REGION", "us-east-1")
    bucket = require("SPACES_BUCKET")
    key = require("SPACES_KEY")

    s3 = boto3.client(
        "s3",
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=require("SPACES_ACCESS_KEY_ID"),
        aws_secret_access_key=require("SPACES_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4"),
    )

    local_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading s3://{bucket}/{key} -> {local_path}")
    s3.download_file(bucket, key, str(local_path))


def get_parquet_path() -> pathlib.Path:
    local_override = os.getenv("LOCAL_PARQUET")
    if local_override:
        p = pathlib.Path(local_override).expanduser().resolve()
        if not p.exists():
            raise SystemExit(f"LOCAL_PARQUET points to missing file: {p}")
        print(f"Using local parquet: {p}")
        return p

    p = (TMP_DIR / "data.parquet").resolve()
    download_from_spaces(p)
    return p


def build_sql(parquet_path: pathlib.Path, null_to_zero: bool = False) -> str:
    parquet_file = str(parquet_path.resolve()).replace("\\", "/")

    def pivot_expr(src: str) -> str:
        core = f"MAX(CASE WHEN source = '{src}' THEN count END)"
        return f"COALESCE({core}, 0) AS {src}" if null_to_zero else f"{core} AS {src}"

    pivot_cols = ",\n      ".join(pivot_expr(src) for src in SOURCES)

    # Columns list (used when copying yesterday -> today)
    metric_cols = ",\n      ".join(SOURCES)

    return f"""
    WITH params AS (
      SELECT
        CURRENT_DATE AS today,
        CURRENT_DATE - INTERVAL 1 DAY AS yesterday
    ),
    base AS (
      SELECT
        clientAccount.tradeName AS client,
        ingestion_ts AS ingested_at,
        CAST(ingestion_ts AS DATE) AS date,
        ingestion_ts,
        counters
      FROM read_parquet('{parquet_file}')
      WHERE clientAccount.tradeName IS NOT NULL
        AND ingestion_ts IS NOT NULL
    ),
    earliest AS (
      SELECT *
      FROM base
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY client, date
        ORDER BY ingestion_ts ASC
      ) = 1
    ),
    exploded AS (
      SELECT
        e.client,
        e.ingested_at,
        e.date,
        x.source AS source,
        x.count  AS count
      FROM earliest e
      LEFT JOIN UNNEST(e.counters) AS t(x) ON TRUE
    ),
    daily AS (
      SELECT
        client,
        ingested_at,
        date,
        {pivot_cols}
      FROM exploded
      GROUP BY client, date, ingested_at
    ),
    -- NEW: latest snapshot of yesterday per client (used only for filling today)
    yesterday_latest AS (
      SELECT b.*
      FROM base b
      JOIN params p ON b.date = p.yesterday
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY b.client, b.date
        ORDER BY b.ingestion_ts DESC
      ) = 1
    ),
    yesterday_latest_exploded AS (
      SELECT
        y.client,
        y.ingested_at,
        y.date,
        x.source AS source,
        x.count  AS count
      FROM yesterday_latest y
      LEFT JOIN UNNEST(y.counters) AS t(x) ON TRUE
    ),
    yesterday_latest_daily AS (
      SELECT
        client,
        ingested_at,
        date,
        {pivot_cols}
      FROM yesterday_latest_exploded
      GROUP BY client, date, ingested_at
    ),
    missing_today AS (
      -- clients that exist yesterday but not today
      SELECT y.*
      FROM yesterday_latest_daily y
      JOIN params p ON y.date = p.yesterday
      LEFT JOIN daily t
        ON t.client = y.client AND t.date = p.today
      WHERE t.client IS NULL
    ),
    fill_today AS (
      SELECT
        client,
        ingested_at,  -- yesterday's latest ingested_at
        (SELECT today FROM params) AS date,
        {metric_cols}
      FROM missing_today
    )
    SELECT * FROM daily
    UNION ALL
    SELECT * FROM fill_today
    ORDER BY date, client
    """.strip()


def export_csv(parquet_path: pathlib.Path) -> pathlib.Path:
    today = date.today().isoformat()
    out_path = (OUT_DIR / f"holded_counters_{today}.csv").resolve()

    con = duckdb.connect(database=":memory:")
    sql = build_sql(parquet_path, null_to_zero=False).strip().rstrip(";")

    out_file = str(out_path).replace("\\", "/")
    con.execute(f"COPY ({sql}) TO '{out_file}' (HEADER, DELIMITER ',');")

    return out_path


def main() -> None:
    parquet_path = get_parquet_path()
    out_csv = export_csv(parquet_path)
    print(f"Saved: {out_csv}")


if __name__ == "__main__":
    main()
