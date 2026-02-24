import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
import pandas as pd
import boto3
from io import BytesIO


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
        holded_jwt = os.environ.get("HOLDED_JWT")
        holded_phpsessid = os.environ.get("HOLDED_PHPSESSID")
        do_spaces_key = os.environ.get("SPACES_ACCESS_KEY_ID")
        do_spaces_secret = os.environ.get("SPACES_SECRET_ACCESS_KEY")
        do_spaces_endpoint = os.environ.get(
            "SPACES_ENDPOINT", "https://fra1.digitaloceanspaces.com"
        )
        do_spaces_bucket = os.environ.get("SPACES_BUCKET", "asevia-prod-data-raw")

        # Validate required secrets
        if not all([holded_jwt, holded_phpsessid, do_spaces_key, do_spaces_secret]):
            raise Exception("Missing required environment variables")

        # Query Holded API
        url = "https://app.holded.com/internal/partner"
        headers = {
            "accept": "application/json",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "priority": "u=1, i",
            "referer": "https://app.holded.com/partners/scanners",
        }
        cookies = {
            "hat": holded_jwt,
            "PHPSESSID": holded_phpsessid,
            # TSCFO
            "accountid": "64f9d4f4f125f1f6660114df",
        }

        response = requests.get(url, headers=headers, cookies=cookies, timeout=25)
        response.raise_for_status()

        partner_data = response.json()
        partnerships = partner_data.get("partnerships", [])

        if not partnerships:
            raise Exception("No partnerships data found")

        # Convert to DataFrame and then to Parquet
        df = pd.DataFrame(partnerships)

        # Add ingestion_ts field - same timestamp used in filename
        now = datetime.now(tz=ZoneInfo("Europe/Madrid"))
        df['ingestion_ts'] = pd.to_datetime(now)

        # Generate file path with current date and epoch
        date_str = now.strftime("%Y%m%d")
        epoch = int(now.timestamp())
        s3_key = f"accounting/holded_status/{date_str}/{epoch}_0.parquet"

        # Convert DataFrame to Parquet in memory
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow", index=False)
        parquet_buffer.seek(0)

        # Upload to Digital Ocean Spaces (S3-compatible)
        s3_client = boto3.client(
            "s3",
            endpoint_url=do_spaces_endpoint,
            aws_access_key_id=do_spaces_key,
            aws_secret_access_key=do_spaces_secret,
            region_name="fra1",
        )

        s3_client.put_object(
            Bucket=do_spaces_bucket,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        return {
            "statusCode": 200,
            "body": {
                "message": "Successfully synced Holded accounting status",
                "records_processed": len(partnerships),
                "file_path": f"s3://{do_spaces_bucket}/{s3_key}",
                "timestamp": now.isoformat(),
            },
        }

    except requests.exceptions.RequestException as e:
        raise Exception("Holded API request failed") from e

    except Exception as e:
        raise Exception("Internal error") from e


# Execute main function if run as a script
if __name__ == "__main__":
    
    result = main({})