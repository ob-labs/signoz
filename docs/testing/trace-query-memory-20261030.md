# Native Trace pagination memory: seekdb and OceanBase CE validation

Date: 2026-10-30. Local implementation and retest evidence; no image publication or production deployment is implied. [Machine-readable results and tested source hashes](evidence/trace-query-memory-20261030.json) accompany this report.

## Problem and change

The native Trace page query derived filters, facets and scope from each candidate Span's full JSON attributes/payload. Large non-page inputs therefore consumed query resources even when the caller requested only 50 Traces.

The implementation adds stored `page_dimensions_v1` and `page_source_bytes_v1` columns. Full-window filtering, scope validation and facets read compact metadata; raw Span JSON is assembled for the selected page. Historical rows are backfilled and updates recompute the metadata. Definitions are checked strictly; missing/incompatible helpers fail explicitly rather than invoking an unbounded fallback.

Metadata and encoded-page budgets are checked before JSON aggregation, in the same repeatable-read transaction as the page query. Native reads, including source-page recovery, share two admission slots per process. Source pages preflight raw sizes before JSON construction, and complete-window recovery has a 24 MiB response budget. Busy/over-budget queries return explicit HTTP 429/413. Existing exact counts, complete page Traces, cross-Span predicates, authorization scope and tag/null precedence remain covered by tests.

The companion AI Vision change bounds streamed response decoding and cumulative source batches, propagates capacity errors, and configures seekdb/container memory. Deploy matching service versions.

## Direct SQL comparison

Fixture: 5,000 Traces / 5,000 Spans, three-day window, page size 50, one Span per Trace. The first 4,950 input bodies were 64 KiB; the 50 newest were 16 KiB. Input bodies appeared in both attributes and payload. Raw JSON totaled 652,074,800 bytes; stored metadata totaled 1,415,000 bytes.

Four concurrent direct SQL callers, separate database-process restarts:

| Engine / metric | Original SQL | Optimized SQL |
|---|---:|---:|
| seekdb 1.4.0.0: individual elapsed time | 3.198–3.281 s | 1.204–1.243 s |
| seekdb: sampled process RSS peak | 1,020.72 MiB | 600.43 MiB |
| seekdb: sampled cgroup peak | 1,047.56 MiB | 657.51 MiB |
| CE 4.4.2.1: individual elapsed time | 6.762–6.763 s | 1.806–1.825 s |
| CE: sampled observer RSS peak | 3,773.37 MiB | 3,074.53 MiB |
| CE: RSS increase above starting sample | 1,033.55 MiB | 389.72 MiB |
| CE: sampled cgroup peak | 3,942.10 MiB | 3,292.73 MiB |

Both engines returned complete IDs 4950–4999, exact count 5,000 and all 16 KiB page inputs; each SQL envelope was 1,689,241 bytes. CE ran two additional warm rounds per variant: original 4.105–4.551 s versus optimized 0.639–0.672 s. All 24 CE responses shared canonical hash `8f3de6d5a1f9c9204ec22d1d7af7a1d3b94da2ab05a0cc18c5828c35a702dd6d`. Both containers recorded zero OOM events.

The actual plans use `page_dimensions_v1` for filter/facet/scope expressions. Metadata scans, sorting and aggregation still scale with the candidate window; this is not an O(page-size) claim for the complete query.

## Environments and methodology

- Native Linux ARM64, two-CPU database cgroup quota, Go 1.25.14 for tests.
- seekdb: fixed image `oceanbase/seekdb@sha256:4db4961869d554313520d6081fca29265ba6da6db7313311bd8acbed771a956b5ad`, 4 GiB container, verified `memory_budget=2G`.
- CE: fixed image `oceanbase/oceanbase-ce@sha256:eaa2c3b3513fa21e4a658f197c1deb93c9d6b6eb742cfa5ef0f085f82b9cec54`, 8 GiB container, verified `memory_limit=6G`, `system_memory=1G`, 3 GiB test tenant. An isolated host bind mount used a 2 GiB data file and 4 GiB log allocation after the Docker VM lacked space for the initial allocation.
- The original variant ran first. Process restarts did not clear the host page cache; RSS/cgroup sampling can miss brief peaks. No integration-test traffic ran during the comparison.
- The harness uses the production SQL builder. seekdb timing includes encoded-page preflight and the page query; CE also includes metadata-size preflight. Schema checks are exercised by integration tests, not the timed SQL.
- Four direct SQL callers intentionally bypass the two-slot HTTP admission gate. Results are not full API latency, P95, production capacity or a seekdb-versus-CE ranking; engine budgets and storage differ.

## Real database validation

Both complete packages passed with `SIGNOZ_TEST_OCEANBASE_DSN` set to an isolated real engine:

```sh
go test ./pkg/modules/aivision ./pkg/ingest -count=1
```

For the CE run, equivalent package test binaries were built with `go test -c` and executed with `-test.v -test.count=1` from each package directory. The results were 188 top-level passing tests in `aivision` and 52 in `ingest`, including 11 real CE cases. Only the opt-in SQL export helper was skipped. Covered behaviors include historical metadata backfill/update, explicit-null/tag precedence, complete and stable pagination, scope retagging, session/read-model semantics, maintenance concurrency/lifecycle and recovery windows. The optional external Python differential oracle was not configured.

The 5,000 CE fixture rows existed before adding the helpers. The production migration and strict read-back returned `ready=true`. The wrapper took 10.53 seconds including migration-client container startup/read-back; it is not a standalone ALTER measurement. No CE-specific source changes were needed. Source hashes match the implementation used for both engine retests.

To export the current production page SQL without recreating its filter logic:

```sh
mkdir -p /tmp/trace-memory-sql
SIGNOZ_TEST_MEMORY_SQL_OUT=/tmp/trace-memory-sql \
  go test ./pkg/modules/aivision -run '^TestExportTracePageMemorySQL$' -count=1
```

The generated SQL targets an isolated synthetic fixture; do not point a destructive fixture loader at production data.

## Upgrade and limits

The additive initializer has a two-minute budget and does not overwrite incompatible columns. Migration failure leaves ingestion available, while pagination explicitly requires completed metadata migration. Large existing tables require a planned maintenance window, ALTER privileges and disk/CPU capacity. New stored expressions shift work to writes; ingestion-throughput impact was not measured.

Per-process admission multiplies with replicas. Byte limits deliberately reject requests that cannot fit; they do not guarantee that every three-day range succeeds. CE keeps `memory_limit`; seekdb uses its separate `memory_budget` setting. Container caps remain separate from database configuration.

Other engine versions, production traffic, many-Spans-per-Trace memory behavior at scale, distributed CE, x86 performance, fresh deployment images, browser behavior and multi-day resident-projector runs remain unverified. The original 7.8 GiB/OOM incident was not reproduced by this fixture. No frontend changes were made.
