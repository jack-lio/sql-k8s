#!/bin/bash
# Run by the minio-init Job (see init-job.yaml). Safe to run repeatedly.
set -euo pipefail

BUCKET=aqi-images
# Credentials via environment, not command-line arguments.
export MC_HOST_local="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio.minio.svc:9000"

until mc ready local >/dev/null 2>&1; do
    echo "waiting for MinIO..."
    sleep 3
done

mc mb --ignore-existing "local/$BUCKET"

# Hard cap enforced by MinIO (the PVC size is not enforced on hostPath).
mc quota set "local/$BUCKET" --size 5GiB

# Delete images after 90 days; add the rule only once.
if ! mc ilm rule ls "local/$BUCKET" --json 2>/dev/null | grep -q '"Days":90'; then
    mc ilm rule add "local/$BUCKET" --expire-days 90
fi

# The Airflow DAG gets its own user that can only use this bucket.
cat > /tmp/policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
     "Resource": ["arn:aws:s3:::$BUCKET"]},
    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
     "Resource": ["arn:aws:s3:::$BUCKET/*"]}
  ]
}
EOF
mc admin policy create local aqi-images-rw /tmp/policy.json
mc admin user add local "$AIRFLOW_ACCESS_KEY" "$AIRFLOW_SECRET_KEY"
mc admin policy attach local aqi-images-rw --user "$AIRFLOW_ACCESS_KEY" 2>&1 \
    | grep -v -i "already" || true

mc quota info "local/$BUCKET"
mc ilm rule ls "local/$BUCKET"
mc admin user info local "$AIRFLOW_ACCESS_KEY"
echo "minio-init done"
