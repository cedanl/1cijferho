#!/usr/bin/env bash
#
# Provision a least-privilege CI user + bucket on the SURF object store
# (Ceph RGW, https://objectstore.surf.nl), following the CEDA
# objectstore-onboarding pattern.
#
# Contains NO credentials. It uses a *root* aws-cli profile that already
# exists on the operator's machine (never in CI) to create a non-root,
# S3-only user and a dedicated CI bucket, then prints the non-root key ONCE.
# Copy that key straight into the GitHub secrets (see
# docs/ci-surf-object-store.md) — it is not stored anywhere by this script.
#
# Usage:
#   scripts/provision_ci_objectstore.sh <root-profile> <ci-user> <ci-bucket>
#
# Example:
#   scripts/provision_ci_objectstore.sh ceda-root 1cijferho-ci 1cijferho-ci-test
#
set -euo pipefail

ROOT_PROFILE="${1:?usage: provision_ci_objectstore.sh <root-profile> <ci-user> <ci-bucket>}"
CI_USER="${2:?missing <ci-user>}"
CI_BUCKET="${3:?missing <ci-bucket>}"

S3_POLICY_ARN="arn:aws:iam::aws:policy/AmazonS3FullAccess"

echo ">> Using root profile '${ROOT_PROFILE}' to provision CI user '${CI_USER}' and bucket '${CI_BUCKET}'."
echo ">> (The root profile must already be configured with endpoint_url https://objectstore.surf.nl.)"
echo

# Sanity: root profile authenticates.
if ! aws --profile "${ROOT_PROFILE}" s3 ls >/dev/null 2>&1; then
  echo "!! Root profile '${ROOT_PROFILE}' cannot authenticate. Configure it first:" >&2
  echo "     aws configure --profile ${ROOT_PROFILE} set endpoint_url https://objectstore.surf.nl" >&2
  exit 1
fi

# 1. Non-root user (idempotent).
if aws --profile "${ROOT_PROFILE}" iam get-user --user-name "${CI_USER}" >/dev/null 2>&1; then
  echo ">> User '${CI_USER}' already exists — reusing."
else
  echo ">> Creating user '${CI_USER}'..."
  aws --profile "${ROOT_PROFILE}" iam create-user --user-name "${CI_USER}" >/dev/null
fi

# 2. S3-only policy (NOT IAM) — least privilege.
echo ">> Attaching ${S3_POLICY_ARN} (S3 only, no IAM)..."
aws --profile "${ROOT_PROFILE}" iam attach-user-policy \
  --user-name "${CI_USER}" --policy-arn "${S3_POLICY_ARN}" >/dev/null

# 3. Dedicated CI bucket (idempotent).
if aws --profile "${ROOT_PROFILE}" s3 ls "s3://${CI_BUCKET}" >/dev/null 2>&1; then
  echo ">> Bucket '${CI_BUCKET}' already exists — reusing."
else
  echo ">> Creating bucket '${CI_BUCKET}'..."
  aws --profile "${ROOT_PROFILE}" s3 mb "s3://${CI_BUCKET}" >/dev/null
fi

# 4. Fresh access key for the CI user — printed ONCE.
echo ">> Creating an access key for '${CI_USER}'..."
KEY_JSON="$(aws --profile "${ROOT_PROFILE}" iam create-access-key --user-name "${CI_USER}")"
ACCESS_KEY_ID="$(printf '%s' "${KEY_JSON}" | python3 -c 'import sys,json;print(json.load(sys.stdin)["AccessKey"]["AccessKeyId"])')"
SECRET_ACCESS_KEY="$(printf '%s' "${KEY_JSON}" | python3 -c 'import sys,json;print(json.load(sys.stdin)["AccessKey"]["SecretAccessKey"])')"

cat <<EOF

============================================================================
 CI credentials for '${CI_USER}' — shown ONCE, not stored by this script.
 Set them as GitHub repo secrets now (never commit them):

   gh secret set SURF_S3_ENDPOINT   --body "https://objectstore.surf.nl"
   gh secret set SURF_S3_ACCESS_KEY --body "${ACCESS_KEY_ID}"
   gh secret set SURF_S3_SECRET_KEY --body "${SECRET_ACCESS_KEY}"
   gh secret set SURF_S3_BUCKET     --body "${CI_BUCKET}"

 If this user had an older key you are replacing, delete it after confirming
 the new one works:
   aws --profile ${ROOT_PROFILE} iam list-access-keys --user-name ${CI_USER}
   aws --profile ${ROOT_PROFILE} iam delete-access-key --user-name ${CI_USER} --access-key-id <OLD_ID>
============================================================================
EOF
