#!/usr/bin/env bash
# Simple uploader: find local transaction-history file and upload to S3 as data/home_expenses.csv (overwrite)
# Usage examples:
#   ./scripts/upload_home_expenses.sh -b my-bucket-name
#   ./scripts/upload_home_expenses.sh -b my-bucket -s data/20250928_transaction-history_wise.csv
#   ./scripts/upload_home_expenses.sh -b my-bucket -p streamlit-reader
#   AWS_PROFILE=streamlit-reader ./scripts/upload_home_expenses.sh -b my-bucket
#
# Requirements: aws CLI configured (aws cli v2 recommended). IAM identity used must have s3:PutObject on the target key.
set -euo pipefail

err() { echo "ERROR: $*" >&2; exit 1; }

# Defaults
DEFAULT_KEY="data/home_expenses.csv"
SOURCE=""
BUCKET="${S3_BUCKET:-}"
KEY="$DEFAULT_KEY"
PROFILE=""
REGION="${AWS_REGION:-}"

usage() {
  cat <<EOF
Usage: $0 -b <s3-bucket> [-s <source-file>] [-k <s3-key>] [-p <aws-profile>] [-r <aws-region>]

Options:
  -b  S3 bucket name (required unless S3_BUCKET env var is set)
  -s  Local source CSV file to upload. If omitted, the script will pick the newest file matching data/*transaction-history*.
  -k  Destination S3 key (default: ${DEFAULT_KEY})
  -p  AWS CLI profile to use (optional). Alternatively set AWS_PROFILE or export AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.
  -r  AWS region (optional). If omitted, AWS CLI config or env will determine it.

Examples:
  # use default discovery to find the latest transaction-history file and upload to the bucket
  ./scripts/upload_home_expenses.sh -b home-maintenance-339713077083-d7f0f7

  # explicitly upload a named file and use a profile
  ./scripts/upload_home_expenses.sh -b home-maintenance-339713077083-d7f0f7 -s data/20250928_transaction-history_wise.csv -p streamlit-reader

EOF
  exit 1
}

while getopts ":b:s:k:p:r:h" opt; do
  case ${opt} in
    b) BUCKET="$OPTARG" ;;
    s) SOURCE="$OPTARG" ;;
    k) KEY="$OPTARG" ;;
    p) PROFILE="$OPTARG" ;;
    r) REGION="$OPTARG" ;;
    h) usage ;;
    \?) err "Invalid option: -$OPTARG" ;;
    :) err "Missing argument for -$OPTARG" ;;
  esac
done

if [ -z "$BUCKET" ]; then
  err "S3 bucket is required. Provide -b BUCKET or set S3_BUCKET env var."
fi

# If source not provided, find the newest file matching transaction-history in data/
if [ -z "$SOURCE" ]; then
  # find via ls -t (newest first). Redirect errors if the pattern doesn't match.
  SOURCE=$(ls -t data/*transaction-history* 2>/dev/null || true)
  if [ -n "$SOURCE" ]; then
    # If multiple, take the first (newest)
    SOURCE=$(echo "$SOURCE" | head -n1)
  fi
fi

if [ -z "$SOURCE" ]; then
  err "No source file provided and none found in data/*transaction-history*. Pass -s <file>."
fi

if [ ! -f "$SOURCE" ]; then
  err "Source file does not exist: $SOURCE"
fi

# Build aws command parts
AWS_EXTRA=()
if [ -n "$PROFILE" ]; then
  AWS_EXTRA+=(--profile "$PROFILE")
fi
if [ -n "$REGION" ]; then
  AWS_EXTRA+=(--region "$REGION")
fi

DEST="s3://${BUCKET}/${KEY}"

echo "Uploading '$SOURCE' → $DEST"
# Use server-side encryption and set content-type, and overwrite by default.
# Note: remove --sse if not desired.
aws s3 cp "$SOURCE" "$DEST" "${AWS_EXTRA[@]}" --acl bucket-owner-full-control --content-type "text/csv" --sse AES256

echo "Upload finished. Verifying..."
if aws s3 ls "$DEST" "${AWS_EXTRA[@]}" >/dev/null 2>&1; then
  echo "OK: $DEST exists in S3"
  aws s3 ls "$DEST" "${AWS_EXTRA[@]}"
  echo "Done."
else
  err "Upload failed or object not found after upload."
fi