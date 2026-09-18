"""
Upload logistics datasets to Amazon S3.

This module uploads raw and processed CSV datasets to an S3 bucket.
Credentials are read from the environment/AWS configuration rather than
being hard-coded in the source code.

Example:
    python storage/s3/upload_to_s3.py --bucket my-logistics-bucket

Optional:
    python storage/s3/upload_to_s3.py --bucket my-logistics-bucket --prefix logistics
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
except ImportError:
    boto3 = None
    BotoCoreError = ClientError = NoCredentialsError = Exception


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Upload logistics raw and processed data to Amazon S3."
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("S3_BUCKET"),
        help="S3 bucket name. Can also be provided through S3_BUCKET.",
    )
    parser.add_argument(
        "--prefix",
        default=os.getenv("S3_PREFIX", "logistics"),
        help="Base S3 prefix/folder.",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"),
        help="AWS region. Optional if configured in the AWS environment.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show files that would be uploaded without uploading them.",
    )
    return parser.parse_args()


def collect_csv_files() -> list[tuple[Path, str]]:
    """
    Collect raw and processed CSV files.

    Returns:
        A list of (local_file, s3_key) pairs.
    """
    files: list[tuple[Path, str]] = []

    if RAW_DIR.exists():
        for file_path in sorted(RAW_DIR.glob("*.csv")):
            files.append((file_path, f"raw/{file_path.name}"))

    if PROCESSED_DIR.exists():
        for file_path in sorted(PROCESSED_DIR.glob("*.csv")):
            files.append((file_path, f"processed/{file_path.name}"))

    return files


def create_s3_client(region: str | None = None):
    """Create an S3 client using the standard AWS credential chain."""
    if boto3 is None:
        raise RuntimeError(
            "boto3 is not installed. Install it with: pip install boto3"
        )

    if region:
        return boto3.client("s3", region_name=region)

    return boto3.client("s3")


def upload_file(
    s3_client,
    local_file: Path,
    bucket: str,
    s3_key: str,
) -> None:
    """Upload one local file to S3."""
    s3_client.upload_file(str(local_file), bucket, s3_key)
    print(f"Uploaded: {local_file} -> s3://{bucket}/{s3_key}")


def main() -> int:
    """Run the S3 upload workflow."""
    args = parse_args()

    if not args.bucket:
        print(
            "Error: S3 bucket is required. "
            "Use --bucket BUCKET_NAME or set S3_BUCKET."
        )
        return 1

    files = collect_csv_files()

    if not files:
        print("No CSV files found in data/raw or data/processed.")
        return 0

    print(f"Project root: {PROJECT_ROOT}")
    print(f"S3 bucket:    {args.bucket}")
    print(f"S3 prefix:    {args.prefix}")
    print(f"Files found:  {len(files)}")

    if args.dry_run:
        print("\nDry run - no files will be uploaded:")
        for local_file, relative_key in files:
            s3_key = f"{args.prefix.rstrip('/')}/{relative_key}"
            print(f"  {local_file} -> s3://{args.bucket}/{s3_key}")
        return 0

    try:
        s3_client = create_s3_client(args.region)

        for local_file, relative_key in files:
            s3_key = f"{args.prefix.rstrip('/')}/{relative_key}"
            upload_file(s3_client, local_file, args.bucket, s3_key)

    except (NoCredentialsError, ClientError, BotoCoreError) as exc:
        print(f"AWS/S3 error: {exc}")
        return 1
    except RuntimeError as exc:
        print(f"Configuration error: {exc}")
        return 1

    print("\nS3 upload completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
