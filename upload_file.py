#!/usr/bin/env python3
"""
Upload local file matching "*transaction-history*" in ./data to S3 as data/home_expenses.csv.
Usage examples (from repo root):
  # use AWS profile (recommended)
  AWS_PROFILE=streamlit-reader python3 scripts/upload_home_expenses.py --bucket home-maintenance-339713077083-d7f0f7

  # use env creds (export AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION)
  python3 scripts/upload_home_expenses.py --bucket home-maintenance-339713077083-d7f0f7
"""
import argparse
import sys
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

DEFAULT_S3_KEY = "data/home_expenses.csv"
LOCAL_DIR = Path("data")

def find_local_file():
    if not LOCAL_DIR.exists():
        return None
    for p in sorted(LOCAL_DIR.iterdir()):
        if p.is_file() and "transaction-history" in p.name:
            return p
    return None

def upload_file(filename: Path, bucket: str, key: str, profile: str = None, region: str = None):
    # Create session: boto3 will pick env or profile
    session_kwargs = {}
    if profile:
        session_kwargs["profile_name"] = profile
    if region:
        session_kwargs["region_name"] = region
    session = boto3.session.Session(**session_kwargs) if session_kwargs else boto3.session.Session()
    s3 = session.client("s3")

    try:
        print(f"Uploading {filename} -> s3://{bucket}/{key} (overwrite if exists)...")
        s3.upload_file(str(filename), bucket, key)
        print("Upload complete.")
    except ClientError as e:
        print("S3 upload failed:", e, file=sys.stderr)
        return 2
    return 0

def main():
    parser = argparse.ArgumentParser(description="Upload transaction-history file to S3 as data/home_expenses.csv")
    parser.add_argument("--bucket", "-b", required=True, help="S3 bucket name")
    parser.add_argument("--key", "-k", default=DEFAULT_S3_KEY, help=f"S3 key to upload to (default: {DEFAULT_S3_KEY})")
    parser.add_argument("--profile", "-p", default=None, help="AWS CLI profile to use (optional)")
    parser.add_argument("--region", "-r", default=None, help="AWS region override (optional)")
    args = parser.parse_args()

    local = find_local_file()
    if local is None:
        print(f"No file found in {LOCAL_DIR} matching '*transaction-history*'. Aborting.", file=sys.stderr)
        return 1

    print(f"Found local file: {local}")
    rc = upload_file(local, args.bucket, args.key, profile=args.profile, region=args.region)
    if rc != 0:
        return rc

    # verify
    session_kwargs = {}
    if args.profile:
        session_kwargs["profile_name"] = args.profile
    if args.region:
        session_kwargs["region_name"] = args.region
    session = boto3.session.Session(**session_kwargs) if session_kwargs else boto3.session.Session()
    s3 = session.client("s3")
    try:
        resp = s3.head_object(Bucket=args.bucket, Key=args.key)
        size = resp.get("ContentLength")
        print(f"Verified s3://{args.bucket}/{args.key} exists (size={size} bytes).")
    except ClientError as e:
        print("Verification failed:", e, file=sys.stderr)
        return 3

    return 0

if __name__ == "__main__":
    sys.exit(main())