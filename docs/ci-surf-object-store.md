# Testing the S3 backend in CI (SURF object store)

CI exercises the generic S3 backend (`eencijferho.io.backends.s3`) at two levels:

| Workflow | Target | Credentials | Runs when |
|---|---|---|---|
| `s3.yml` | Throwaway **MinIO** container | none | every push / PR (wired into `ci.yml`) |
| `s3-surf.yml` | **Real SURF** Ceph RADOS-Gateway | GitHub secrets | `main` push + manual `workflow_dispatch`, **only if secrets are set** |

> **⚠️ Network limitation — `s3-surf.yml` does not work on GitHub-hosted
> runners.** `objectstore.surf.nl` is only reachable from the SURF network /
> eduVPN. A GitHub-hosted runner has a public cloud IP with no VPN, so its
> connections are dropped and every test fails with
> `botocore.exceptions.EndpointConnectionError` (verified: a full run hung
> through boto3's retries for 2h42m before the backend's connect timeout was
> added). **Treat the SURF suite as a local/VPN-only check** — run it from a
> machine on eduVPN (see [Running against SURF locally](#running-against-surf-locally)).
> To run it in CI you would need a **self-hosted runner inside the SURF
> network**; `s3-surf.yml` is kept (manual-dispatch + `main`) for that future
> or for anyone wiring such a runner. The `S3Backend` now caps connect time and
> retries, so if it *is* run without access it fails in ~30s instead of hours.

`s3.yml` proves the code path on every PR without any external dependency
(MinIO is S3-compatible). `s3-surf.yml` proves real-world compatibility against
the actual SURF object store, and **skips itself cleanly** until a utility
account is provisioned — so it never blocks merges and never fails fork PRs
(which don't receive secrets).

Both jobs run the same test file (`tests/integration/test_s3_backend.py`). The
target is chosen by the `S3_TEST_*` environment variables (see
`tests/integration/conftest.py`): when `S3_TEST_ENDPOINT` is set the tests hit
that endpoint, otherwise they fall back to the local MinIO container.

## Provisioning a CI utility account (without leaking credentials)

The SURF object store is Ceph RGW at `https://objectstore.surf.nl`. The
least-privilege pattern (from the CEDA `objectstore-onboarding` skill) is: a
**root** credential — kept only on an admin's machine — creates a **non-root,
S3-only** user, and *only that non-root key* is given to CI. The public repo
never contains any key; it references GitHub secrets by name.

```
SURF admin ──emails──> root S3 key  (admin laptop only, never in CI)
                          │
                          ├─ create non-root user  1cijferho-ci   (S3FullAccess, NOT IAM)
                          ├─ create CI-only bucket  1cijferho-ci-test
                          └─ non-root key ──> GitHub encrypted secrets ──> s3-surf.yml
```

### Steps (run by a CEDA admin who holds the root key)

Account creation is manual on SURF's side — request it via the SURF servicedesk
first (see the `objectstore-onboarding` skill). Once you have the emailed root
S3 access + secret key, run the helper:

```bash
# 1. Configure + rotate the root key (it travelled over email), then:
aws configure --profile ceda-root set aws_access_key_id     <ROOT_ACCESS_KEY>
aws configure --profile ceda-root set aws_secret_access_key <ROOT_SECRET_KEY>
aws configure --profile ceda-root set endpoint_url          https://objectstore.surf.nl
aws configure --profile ceda-root set region                us-east-1

# 2. Provision the non-root CI user + bucket and print the CI key (once):
scripts/provision_ci_objectstore.sh ceda-root 1cijferho-ci 1cijferho-ci-test
```

The script creates the user with `AmazonS3FullAccess` (S3 yes, IAM no), makes
the bucket, and prints the **non-root** access/secret key **once** — copy it
straight into the secret commands below, then it's gone.

> **Why non-root + S3-only:** if the CI key ever leaked, its blast radius is one
> throwaway bucket — it cannot create users, change policies, or escalate. The
> root key, which *can*, never touches CI.

## Configuring the GitHub secrets

Set them with `gh` (encrypts client-side before upload — the value never appears
in the repo, in logs, or to fork PRs):

```bash
gh secret set SURF_S3_ENDPOINT   --body "https://objectstore.surf.nl"
gh secret set SURF_S3_ACCESS_KEY --body "<1cijferho-ci access key>"
gh secret set SURF_S3_SECRET_KEY --body "<1cijferho-ci secret key>"
gh secret set SURF_S3_BUCKET     --body "1cijferho-ci-test"
# optional: gh secret set SURF_S3_REGION --body "us-east-1"
```

Or via the UI: **Settings → Secrets and variables → Actions → New repository
secret**.

| Secret | Value | Required |
|---|---|---|
| `SURF_S3_ENDPOINT` | `https://objectstore.surf.nl` | yes (this is the gate) |
| `SURF_S3_ACCESS_KEY` | non-root CI user access key | yes |
| `SURF_S3_SECRET_KEY` | non-root CI user secret key | yes |
| `SURF_S3_BUCKET` | dedicated CI bucket, e.g. `1cijferho-ci-test` | yes |
| `SURF_S3_REGION` | region label (RGW accepts any) | no (default `us-east-1`) |

**Who can set/overwrite these:** only users with repo **admin** or **maintain**
role. Secrets are **write-only** — once set, nobody (not even an owner) can read
the value back through GitHub; it is only decrypted inside a running job and
masked in logs. Outside contributors can open PRs only from forks, and **fork
PRs never receive secrets**, so a malicious workflow edit in a fork PR has
nothing to steal.

Once `SURF_S3_ENDPOINT` is present, the `check-secrets` gate in `s3-surf.yml`
flips to `configured=true` and the test job runs on the next `main` push or
manual dispatch. Trigger it manually from the **Actions** tab → *S3 backend
tests (SURF object store)* → **Run workflow**.

## Rotating or revoking the CI key

```bash
# rotate: create a new key, update the secret, then delete the old key
aws --profile ceda-root iam create-access-key --user-name 1cijferho-ci
gh secret set SURF_S3_ACCESS_KEY --body "<new id>"
gh secret set SURF_S3_SECRET_KEY --body "<new secret>"
aws --profile ceda-root iam delete-access-key --user-name 1cijferho-ci --access-key-id <old id>
```

Rotate immediately if a key is ever exposed — retiring one takes seconds.

## Running against SURF locally

The same env vars work locally once you have the non-root credentials:

```bash
export S3_TEST_ENDPOINT=https://objectstore.surf.nl
export S3_TEST_ACCESS_KEY=...
export S3_TEST_SECRET_KEY=...
export S3_TEST_BUCKET=1cijferho-ci-test
uv run pytest tests/integration/test_s3_backend.py -v
```

With `S3_TEST_ENDPOINT` unset, the same command falls back to the local MinIO
container (started via docker-compose).
