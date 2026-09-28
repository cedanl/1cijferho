#!/usr/bin/env python
"""
Create the MinIO bucket and make it anonymously readable.

Replaces the old `docker compose` minio-init service, which shelled out to the
`mc` client. That client is no longer obtainable: the mc binary is 410 Gone
from dl.min.io and the `minio/mc` image is gone from Docker Hub. This does the
same two things with the `minio` Python client instead, which the app image
already has installed (Dockerfile installs the `all-backends` extra).

Ran by the `minio-init` service in docker-compose.yml:

    docker compose run --rm minio-init

Environment:
    MINIO_ENDPOINT     host:port of the MinIO server (default minio:9000)
    MINIO_ACCESS_KEY   access key  (default minioadmin)
    MINIO_SECRET_KEY   secret key  (default minioadmin)
    MINIO_BUCKET       bucket to create (default 1cijferho)
    MINIO_SECURE       "true" for TLS  (default false)
    MINIO_PUBLIC       "false" to skip the public-read policy (default true)
"""

import json
import os
import sys
import time

from minio import Minio

# Anonymous download, i.e. the equivalent of `mc anonymous set download`.
# This is what lets a browser fetch an object straight from the bucket without
# credentials, so the Streamlit demo can render a public link.
PUBLIC_DOWNLOAD_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"AWS": ["*"]},
            "Action": ["s3:GetObject"],
            "Resource": ["arn:aws:s3:::*/*"],
        }
    ],
}

TIMEOUT_SECONDS = 30


def main() -> int:
    endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
    bucket = os.environ.get("MINIO_BUCKET", "1cijferho")

    client = Minio(
        endpoint,
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
    )

    # Compose already gates on service_healthy, but this also makes the script
    # usable standalone, so wait rather than crash on a connection refused.
    # Probes list_buckets() (a server-level call) and NOT bucket_exists(),
    # because on a first run the bucket does not exist yet and that would look
    # like "not ready" forever.
    for attempt in range(1, TIMEOUT_SECONDS + 1):
        try:
            client.list_buckets()
            break
        except Exception as exc:  # noqa: BLE001 - any error means "not up yet"
            if attempt == TIMEOUT_SECONDS:
                print(
                    f"ERROR: MinIO at {endpoint} not ready after {TIMEOUT_SECONDS}s: {exc}"
                )
                return 1
            print(f"Waiting for MinIO at {endpoint}... ({attempt})", flush=True)
            time.sleep(1)

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"created bucket {bucket}")
    else:
        print(f"bucket {bucket} already exists")

    if os.environ.get("MINIO_PUBLIC", "true").lower() == "true":
        client.set_bucket_policy(bucket, json.dumps(PUBLIC_DOWNLOAD_POLICY))
        print(f"set anonymous download policy on {bucket}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
