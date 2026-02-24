import os
from datetime import datetime

import boto3
import duckdb


def _init_duckdb_s3_connection(access_key: str, secret_key: str, endpoint: str):
    """
    Initialize a DuckDB in-memory connection configured to access S3-compatible storage
    (DigitalOcean Spaces in this case).
    """
    # Strip scheme from endpoint for DuckDB
    endpoint_no_scheme = endpoint.replace("https://", "").replace("http://", "")

    os.makedirs("/tmp/duckdb_tmp", exist_ok=True) # Ensure temp directory exists

    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    # Performance and resource settings
    con.execute("SET temp_directory='/tmp/duckdb_tmp';") # Spill intermediate data to disk here instead of crashing
    con.execute("SET threads=1;") # Limit to 1 thread to reduce CPU/memory spikes
    con.execute("SET memory_limit='500MB';") # Forces it to spill earlier, before hitting the container limit

    con.execute("SET s3_access_key_id = ?", [access_key])
    con.execute("SET s3_secret_access_key = ?", [secret_key])
    con.execute("SET s3_endpoint = ?", [endpoint_no_scheme])
    # For S3-compatible (MinIO-style / DO Spaces) it's usually safer to use path-style
    con.execute("SET s3_url_style = 'path';")
    con.execute("SET s3_use_ssl = true;")
    con.execute("SET s3_region = 'fra1';")

    return con


def main(args):
    """
    Sync Holded accounting status to Digital Ocean Spaces.

    Args:
        args: Dictionary containing request parameters

    Returns:
        Dictionary with statusCode and body
    """
    try:
        # Get secrets from environment variables
        do_spaces_key = os.environ.get("SPACES_ACCESS_KEY_ID")
        do_spaces_secret = os.environ.get("SPACES_SECRET_ACCESS_KEY")
        do_spaces_endpoint = os.environ.get(
            "SPACES_ENDPOINT", "https://fra1.digitaloceanspaces.com"
        )
        do_spaces_bucket = os.environ.get("SPACES_BUCKET", "asevia-prod-data-raw")

        # Validate required secrets
        if not all([do_spaces_key, do_spaces_secret]):
            raise Exception("Missing required environment variables")

        # S3 client for light metadata checks (listing objects)
        s3_client = boto3.client(
            "s3",
            endpoint_url=do_spaces_endpoint,
            aws_access_key_id=do_spaces_key,
            aws_secret_access_key=do_spaces_secret,
        )

        # Initialize DuckDB connection configured for DO Spaces
        con = _init_duckdb_s3_connection(
            access_key=do_spaces_key, # type: ignore
            secret_key=do_spaces_secret, # type: ignore
            endpoint=do_spaces_endpoint,
        )

        folders = ["accounting/holded_status"] # ["accounting/holded_status", "accounting/inmatic_invoices"]

        total_records_processed = 0
        merged_outputs = {}  # folder -> {"file_path": ..., "records_processed": ...}

        for folder in folders:
            input_glob = f"s3://{do_spaces_bucket}/{folder}/**/*.parquet"

            # 3) Count rows and write merged parquet
            # # Count rows first
            # row_count_res = con.execute(
            #     f"SELECT COUNT(*) FROM read_parquet('{input_glob}')"
            # ).fetchone()

            # row_count = 0 if row_count_res is None else row_count_res[0]
            # if row_count == 0:
            #     # Nothing to merge even though files exist (edge case)
            #     merged_outputs[folder] = {
            #         "file_path": None,
            #         "records_processed": 0,
            #     }
            #     continue

            # To avoid race conditions, first write to a tmp file, the rename
            temp_output_path = f"s3://{do_spaces_bucket}/{folder}_grouped/single_tmp.parquet"

            # Merge all files into one parquet using DuckDB COPY
            con.execute(
                f"""
                COPY (
                    SELECT * FROM read_parquet('{input_glob}')
                ) TO '{temp_output_path}' (FORMAT PARQUET);
                """
            )

            tmp_key = f"{folder}_grouped/single_tmp.parquet"
            final_key = f"{folder}_grouped/single.parquet"

            copy_source = {"Bucket": do_spaces_bucket, "Key": tmp_key}
            s3_client.copy_object(
                Bucket=do_spaces_bucket,
                CopySource=copy_source,
                Key=final_key,
            )

            # Delete tmp to avoid unnecessary storage
            s3_client.delete_object(Bucket=do_spaces_bucket, Key=tmp_key)

            # total_records_processed += row_count
            # merged_outputs[folder] = {
            #     "file_path": f"s3://{do_spaces_bucket}/{final_key}",
            #     "records_processed": row_count,
            # }

        return {
            "statusCode": 200,
            "body": {
                "message": "Successfully grouped parquet files",
                # "records_processed": total_records_processed,
                "outputs": merged_outputs,
                "timestamp": datetime.now().isoformat(),
            },
        }

    except Exception as e:
        raise Exception("Internal error") from e


# Execute main function if run as a script
if __name__ == "__main__":
    
    result = main({})
    