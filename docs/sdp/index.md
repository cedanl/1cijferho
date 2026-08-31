# SDP compliance & deployment

Planning material for deploying CEDA repositories on SURF's Developer Platform
(SDP), and the regulatory-gap context driving the tooling choices. This is
background/discussion material — the operational how-to for the storage layer
lives in [Storage backends](../storage-abstraction.md) and
[S3 in CI (SURF)](../ci-surf-object-store.md).

## Documents

| Document | What it is |
|---|---|
| [SDP Gap Analysis](sdp-gap-analysis.html) | SDP mapped against NIS2, EU AI Act, and the Data Act — 14 gaps, 5 critical. Source deck. |
| [Test Case: Repository Deployment](test-case-repository-deployment.html) | `1cijferho` + `NFWA` as the first two repos to deploy on SDP; they surface 4 of the 14 gaps as concrete cases. Source deck. |
| [Object Store for the gaps](object-store-for-gaps.slides.md) | Where an external SURF Object Store (vs. in-cluster SDP MinIO) improves the gap posture — mapped gap by gap. |
| [Questions for the SDA team](object-store-questions-for-sda.md) | The at-rest-encryption / Object-Lock / retention / connectivity questions to resolve before relying on Object Store in the PoC. |

## The short version

The Gap Analysis lists **SURF Object Store** (external Ceph RGW) as suited for
long-term archival and audit-log storage, distinct from **SDP MinIO** (in-cluster
working data). Using the external Object Store is a **genuine improvement** for:

- **GAP-03** (immutable audit logging) — Object Lock / WORM + 10-year retention
  decoupled from the cluster lifecycle;
- **NIS2 21(c)** durability / DR and **Data Act** portability posture.

It is a **conditional** improvement for **GAP-01** (PII storage) — contingent on
documented at-rest encryption *and* the SDP PII-on-production policy roadmap — and
**does not** by itself address GAP-04 / GAP-05 (lineage, model registry), which
are tooling and schema problems. The [questions for the SDA
team](object-store-questions-for-sda.md) are what turn the "may offer better
guarantees" language in the deck into an actual decision.
