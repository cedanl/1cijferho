---
marp: true
theme: default
paginate: true
---

# Object Store vs. SDP MinIO

### Where an external object store improves the gap posture

CEDA / Npuls AI & Data — Draft

<!--
Companion slides to the SDP Gap Analysis deck. Insert after the "Storage
Options" slide. Every claim here maps to a specific GAP-xx from that deck.
The distinction throughout: SDP MinIO = in-cluster Ceph, sized for pipeline
working data; SURF Object Store = external managed Ceph RGW, positioned for
retention, durability, and scale.
-->

---

## Two object stores, two roles

| | SDP MinIO | SURF Object Store |
|---|---|---|
| Type | In-cluster, S3-compatible (Ceph) | External managed service (Ceph RGW) |
| Lifecycle | Tied to the app namespace / cluster | Independent of any cluster |
| Sized for | Pipeline working data, ML artifacts | Long-term archival, audit logs, large datasets |
| Endpoint | Internal | `objectstore.surf.nl` (SURF network / eduVPN) |

Both speak S3, so the application code is identical — `1cijferho` already
selects either via one env var (`STORAGE_BACKEND=s3`). The question is **which
data belongs where**, not whether we can use it.

---

## Where it helps — mapped to the gaps

| Finding | Improvement from external Object Store | Strength |
|---|---|---|
| **GAP-03** Immutable audit logging | Object Lock / WORM + versioning, decoupled from cluster lifecycle; AI Act Art. 12 wants **10-yr** retention | **Strong** |
| **NIS2 21(c)** DR / durability | Managed replication/erasure-coding; survives cluster rebuild (SLA today: "≤24h data may be lost") | **Strong** |
| **Data Act** portability / cloud-switching | Portable S3 API not coupled to one tenancy → reinforces "no lock-in / GOOD" | **Good** |
| **GAP-01** PII storage | Candidate at-rest tier — **only if** encryption-at-rest + policy roadmap confirm it | **Conditional** |
| **GAP-02** Kafka at rest | Doesn't encrypt Kafka, but strengthens the "batch-only, skip Kafka" path | **Indirect** |
| **GAP-04 / GAP-05** Lineage / registry | Can *host* artifacts, but these are tooling/schema gaps, not storage-tier ones | **Neutral** |

---

## GAP-03 — the strongest fit

The Gap Analysis already proposes *"OpenSearch with write-once indices + S3
(MinIO / **SURF Object Store**) archival for long-term retention."*

External Object Store is the better half of that pairing:

- **Object Lock / WORM** → append-only, tamper-evident — the property the audit
  log is *for*
- **10-year retention** (AI Act Art. 12) lives outside the app's own namespace,
  so it survives teardown, cluster rebuild, or a compromised tenant
- Keeping the tamper-proof log **in** the same in-cluster store the app controls
  is a weaker trust boundary

---

## GAP-01 — conditional, not a free unblock

Moving PII off in-cluster MinIO to a managed external service *can* help — but
only if two things hold:

1. **Encryption at rest is documented.** The Gap Analysis itself flags
   *"PostgreSQL / MinIO encryption at rest undocumented"* (NIS2 21(h)). Object
   Store only improves this if its at-rest + SSE-KMS story is stronger **and
   evidenced**.
2. **The prohibition is policy, not just tech.** *"PII storage prohibited on
   production"* is an SDP/SDA decision. Object Store is a candidate tier *if*
   the roadmap allows it — it does not lift the block by itself.

> Verdict: Object Store is part of a **GAP-01 answer**, not the answer.

---

## What we already have (grounds the PoC)

`1cijferho` ships a pluggable storage layer — the generic S3 backend runs
against SURF's Ceph RGW **today**:

- `STORAGE_BACKEND=s3`, path-style + SigV4 (what RADOS-GW requires)
- Integration suite passes against real `objectstore.surf.nl`
- Secrets via SOPS-encrypted K8s secrets (never in-repo)

**Known limitation (verified):** `objectstore.surf.nl` is only reachable from
the SURF network / eduVPN — GitHub-hosted CI runners cannot reach it. Real-SURF
tests are a **local / VPN-only** check, or need a self-hosted runner inside SURF.

---

## Honest bottom line

- **Genuine win:** GAP-03 (audit archival) + durability / portability posture.
- **Conditional win:** GAP-01 — contingent on documented at-rest encryption
  **and** the SDP PII policy roadmap.
- **Indirect:** GAP-02 — reinforces going batch-only rather than solving it.
- **Not a storage win:** GAP-04 / GAP-05 are tooling + schema problems.

The specific guarantees that carry this case — **at-rest encryption, Object Lock
/ WORM, retention SLA** — are asserted at a high level in the deck, not yet
evidenced. Those are the questions for the SDA team → next slide.
