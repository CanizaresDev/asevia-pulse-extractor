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


def require(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise SystemExit(f"Missing required env var: {name}")
    return v


def download_from_spaces(local_path: pathlib.Path) -> None:
    endpoint = require("SPACES_ENDPOINT")  # e.g. https://nyc3.digitaloceanspaces.com
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

    p = TMP_DIR / "data.parquet"
    download_from_spaces(p)
    return p


def export_raw_to_csv(parquet_path: pathlib.Path) -> pathlib.Path:
    today = date.today().isoformat()  # YYYY-MM-DD
    out_path = OUT_DIR / f"raw_{today}.csv"

    con = duckdb.connect(database=":memory:")

    # Stream export to CSV (avoids loading whole file into pandas memory)
    # HEADER + DELIMITER make it a normal CSV.
    con.execute(
        f"""
        COPY (
          SELECT * FROM read_parquet('{parquet_path.as_posix()}')
        )
        TO '{out_path.as_posix()}'
        (HEADER, DELIMITER ',');
        """
    )

    return out_path


def main() -> None:
    parquet_path = get_parquet_path()
    csv_path = export_raw_to_csv(parquet_path)
    print(f"Saved: {csv_path}")


if __name__ == "__main__":
    main()
