# Testing the S3 backend in CI (SURF object store)

CI exercises the generic S3 backend (`eencijferho.io.backends.s3`) at two levels:

| Workflow | Target | Credentials | Runs when |
|---|---|---|---|
| `s3.yml` | Throwaway **MinIO** container | none | every push / PR (wired into `ci.yml`) |
| `s3-surf.yml` | **Real SURF** Ceph RADOS-Gateway | GitHub secrets | `main` push + manual `workflow_dispatch`, **only if secrets are set** |

`s3.yml` proves the code path on every PR without any external dependency
(MinIO is S3-compatible). `s3-surf.yml` proves real-world compatibility against
the actual SURF object store, and **skips itself cleanly** until a utility
account is provisioned — so it never blocks merges and never fails fork PRs
(which don't receive secrets).

Both jobs run the same test file (`tests/integration/test_s3_backend.py`). The
target is chosen by the `S3_TEST_*` environment variables (see
`tests/integration/conftest.py`): when `S3_TEST_ENDPOINT` is set the tests hit
that endpoint, otherwise they fall back to the local MinIO container.

## Provisioning the SURF utility account + bucket

SURF's object store is Ceph RADOS-Gateway, exposed over the S3 API. To enable
`s3-surf.yml`:

1. **Request a utility (service) account and a dedicated CI bucket** via the
   SURF object-store onboarding process (SDP platform onboarding — see the
   `sdp-onboard` skill in the `.github` repo for the access path). Use a bucket
   used **only for CI**, e.g. `1cijferho-ci-test` — the tests write and delete
   throwaway objects under random prefixes, but never mix it with real data.
2. You'll receive an **endpoint URL** (e.g. `https://object.surfsara.nl`), an
   **access key**, and a **secret key** for the utility account.

## Configuring the GitHub secrets

In the repo: **Settings → Secrets and variables → Actions → New repository secret**.

| Secret | Value | Required |
|---|---|---|
| `SURF_S3_ENDPOINT` | e.g. `https://object.surfsara.nl` | yes (this is the gate) |
| `SURF_S3_ACCESS_KEY` | utility-account access key | yes |
| `SURF_S3_SECRET_KEY` | utility-account secret key | yes |
| `SURF_S3_BUCKET` | dedicated CI bucket, e.g. `1cijferho-ci-test` | yes |
| `SURF_S3_REGION` | region label (RADOS-GW accepts any) | no (default `us-east-1`) |

Once `SURF_S3_ENDPOINT` is present, the `check-secrets` gate in `s3-surf.yml`
flips to `configured=true` and the test job runs on the next `main` push or
manual dispatch. Trigger it manually from the **Actions** tab → *S3 backend
tests (SURF object store)* → **Run workflow**.

> **Secrets policy:** never commit these values. In SDP (Kubernetes)
> deployments the same credentials are managed as SOPS-encrypted secrets — see
> the `sdp-secrets-management` skill. GitHub Actions secrets are the CI-only
> equivalent.

## Running against SURF locally

The same env vars work locally once you have the credentials:

```bash
export S3_TEST_ENDPOINT=https://object.surfsara.nl
export S3_TEST_ACCESS_KEY=...
export S3_TEST_SECRET_KEY=...
export S3_TEST_BUCKET=1cijferho-ci-test
uv run pytest tests/integration/test_s3_backend.py -v
```

With `S3_TEST_ENDPOINT` unset, the same command falls back to the local MinIO
container (started via docker-compose).
