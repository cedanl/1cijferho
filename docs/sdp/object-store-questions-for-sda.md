# Object Store — questions for the SDA team

Context: the [SDP Gap Analysis](sdp-gap-analysis.html) names the **SURF Object
Store** (external Ceph RGW, `objectstore.surf.nl`) as suited for *long-term
archival, audit log storage, and large datasets*, distinct from in-cluster **SDP
MinIO** (pipeline working data). Several gap improvements depend on properties
the deck asserts at a high level ("may offer better guarantees") but does not yet
evidence. These are the questions to resolve before leaning on Object Store for
the PoC — especially for GAP-01 (PII) and GAP-03 (audit logging).

The repo already runs the generic S3 backend against `objectstore.surf.nl` (see
[S3 in CI](../ci-surf-object-store.md) and [Storage backends](../storage-abstraction.md)),
so these are operational / guarantee questions, not "can we connect" questions.

## 1. Encryption at rest — blocks GAP-01, GAP-02

- Is data **encrypted at rest** on the SURF Object Store by default? With what
  (Ceph OSD-level, LUKS, per-bucket)? The Gap Analysis flags MinIO/PostgreSQL
  at-rest encryption as *undocumented* — what is documented for Object Store?
- Is **SSE-KMS / SSE-C** (server-side encryption with customer-managed keys)
  supported via the S3 API? If so, which KMS backend, and does it interoperate
  with the "secrets & key management to add" layer (GAP-10, Vault)?
- Where do encryption keys live relative to the data, and who can access them?

## 2. Object Lock / WORM & retention — blocks GAP-03

- Does the RGW deployment support **S3 Object Lock** (governance + compliance
  modes) and **bucket versioning**?
- Can we set **retention of 10 years** (EU AI Act Art. 12) with compliance-mode
  locks that even an admin cannot shorten?
- Is there a **legal-hold** primitive independent of the retention clock?
- Any object-count / object-size ceilings that would bite a decade of
  append-only audit records?

## 3. Durability & DR — improves NIS2 21(c) (currently PARTIAL)

- What is the **durability / availability SLA** for Object Store, vs. the SDP
  SLA quoted in the deck ("at most data from the last 24 hours may be lost")?
- Replication / erasure-coding scheme? Single-site or geo-redundant across SURF
  data centres?
- Is Object Store data **independent of the SDP cluster lifecycle** — i.e. does
  it survive a namespace teardown or cluster rebuild? (This is the core reason
  to prefer it for audit logs.)

## 4. Access model & tenancy — relates to GAP-06 (fine-grained authz)

- Per-bucket / per-prefix **IAM policies**, or only account-level keys?
- Can we separate **Bronze / Silver / Gold** (and the audit-log bucket) into
  distinct policies for least privilege?
- The onboarding pattern we use is root-key → non-root S3-only user → CI. Is that
  the recommended tenancy model, or is there an SDP-integrated identity path
  (SURFconext / SRAM-scoped credentials)?
- Credential rotation: API-driven, or servicedesk-mediated?

## 5. Connectivity from SDP workloads — operational blocker to confirm

- **Verified limitation:** `objectstore.surf.nl` is only reachable from the SURF
  network / eduVPN; GitHub-hosted CI cannot reach it. **Are SDP-hosted
  workloads (K8s pods) on-network for Object Store**, or is extra egress /
  peering needed?
- Recommended endpoint from inside SDP — is there an internal / lower-latency
  route, or is it the same public `objectstore.surf.nl` for pods too?

## 6. Cost, quota, and operational load — feeds the "operational load" research goal

- Cost model: capacity, request, and egress pricing? Egress cost of pulling
  large datasets back into SDP compute?
- Default **quota** and how to raise it for a decade of audit retention plus
  Bronze/Silver/Gold parquet.
- Who operates it — fully SURF-managed, or shared responsibility? Backup of the
  object store itself?

## 7. Lifecycle & tiering — nice-to-have

- **S3 lifecycle rules** (transition, expiration) supported? Useful for
  time-boxing working data while audit logs stay locked for 10 years.
- Any cold / archival storage class cheaper for the long-tail retention?

---

### Decision this unblocks

If **§1 (at-rest + SSE-KMS)** and **§2 (Object Lock + 10-yr retention)** come back
positive, Object Store becomes the concrete home for the **audit-log layer
(GAP-03)** and a viable **PII tier candidate (GAP-01)** in the Sept–Dec 2026 PoC.
If either is negative, we fall back to the alternatives in the Gap Analysis
(OpenSearch write-once, Sigstore Rekor) and keep GAP-01 as an open policy item.
