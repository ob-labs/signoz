# Trace memory optimization follow-up and regression validation

Date: 2026-10-30. This report supplements the [initial memory retest](trace-query-memory-20261030.md). It compares further changes with the already optimized PRs, not with the original payload-scanning implementation. [Machine-readable measurements, source identities and test results](evidence/trace-query-memory-followup-20261030.json) accompany it.

The later [full-stack Linux VM follow-up](trace-query-memory-vm-20261030.md) implements selected directions below and exercises the public HTTP stack on seekdb and OB CE. The measurements and unverified scope in this report remain the historical snapshot.

## Changes retained

**SigNoz:** materialize the compact `base` CTE once per SQL statement. Previously several summaries and facets repeated the same metadata scan and JSON extraction. In the measured page plans, metadata table access nodes fall from six to one on seekdb and seven to one on CE. Raw Span JSON remains outside that CTE and is assembled only for the selected page. The metadata-size preflight and encoded-page preflight remain in the same repeatable-read transaction as the page query; the change does not remove either preflight, alter filters, or truncate results.

**AI Vision:** keep the external-call timer open while consuming and decoding streamed GET/POST responses. The earlier streaming change ended timing at response headers, omitting body-download time and recording body-read failures as successes. A delayed/interrupted real `requests.Response` stream reproduced all four failing GET/POST success/error cases before the fix; they now pass, including response closure and error-counter assertions.

**Regression harness:** repair the opt-in Python differential oracle to call the raw-page adapter directly. Its mock supplies only the raw-page contract, while the public provider now requests read-model/snapshot contracts first. The oracle compares ordered IDs, exact counts and facets declared canonical by the adapter with the independent full-window `NativeTraceWindow`. Legacy any-Span environment and collector-username facets are deliberately omitted by the canonical adapter; explicit database assertions still verify those raw facets. This is storage-plus-adapter coverage, not a full public HTTP-stack test.

## Further SQL performance

Baseline heads: AI Vision `dbcf7290008d1bbe25423d3d2f8c210756ab241f`; SigNoz `fc3ee92c804bd73dc72194fc9a6f9fd2eacbe9e9`. The final production SQL export and arguments exactly match the measured materialization variant. An additional normal `trace_id` equality predicate, retaining the binary predicate, was tested but not retained: it offered little consistent benefit beyond materialization.

The fixture remains 5,000 Traces / 5,000 Spans, a three-day window and a page of 50. Non-page inputs are 64 KiB, page inputs 16 KiB; source JSON totals 652,074,800 bytes and compact metadata 1,415,000 bytes. Each full-page response is 1,689,241 bytes. Linux ARM64, two CPUs per database; seekdb 1.4.0.0 uses a 4 GiB container and `memory_budget=2G`; CE 4.4.2.1 uses an 8 GiB container, `memory_limit=6G`, `system_memory=1G` and the same isolated test tenant/storage configuration as the initial report.

After exploratory/prewarm rounds, eight rounds used a balanced four-variant order. Each batch ran four direct SQL callers. The table shows individual-call medians across 32 observations per variant, including all eight rounds.

| Engine | Current PR baseline | Extra equality | Materialize metadata | Both candidates |
|---|---:|---:|---:|---:|
| seekdb | 404.1 ms | 385.1 ms | **374.1 ms** | 372.7 ms |
| OB CE | 653.0 ms | 643.2 ms | **464.3 ms** | 462.3 ms |

The retained change lowers the observed median by 7.4% on seekdb and 28.9% on CE. All 256 responses from these warm rounds have the same canonical hash, including page content and facets. Each query retains the two resource preflights. Timing ends after SQL result transfer/commit and excludes Python JSON parsing; schema readiness checks and HTTP admission are outside this benchmark. These are local fixture medians, not P95, production capacity or a cross-engine ranking. seekdb still has a slower first follow-up round; raw timings are included so the warm-state variation is visible.

Small-window checks alternate baseline/candidate order for six single-caller rounds per size. Median milliseconds:

| Candidate Spans | seekdb baseline | seekdb candidate | CE baseline | CE candidate |
|---|---:|---:|---:|---:|
| 0 | 2.95 | 2.75 | 19.56 | 7.95 |
| 20 | 10.08 | 9.04 | 24.95 | 14.86 |
| 200 | 31.68 | 24.02 | 49.85 | 26.97 |
| 5,000 | 387.05 | 196.10 | 247.20 | 178.34 |

All 96 small-window comparison responses match their same-window canonical baseline. No latency regression was observed at these sizes. This does not establish behavior at every cardinality or skew. Retained process memory is affected by prior allocations, tests and caches; these shared-process follow-up runs do **not** establish a further RSS reduction. The separate-restart memory results remain in the initial report.

## Functional validation

- AI Vision: **203 tests passed** across streaming/memory budgets, provider and route contracts, pagination, agent scope, facets, read models, OpenAPI, projection, native windows, sessions, managed queries and dashboard consumers. Fourteen are memory/streaming tests. HTTP dependencies in this suite are fixtures/mocks.
- SigNoz: **188 `aivision` tests and 52 `ingest` tests passed on each real database**, including 11 real-database cases. The optional Python differential oracle is enabled on both engines. The opt-in SQL-export helper is skipped in the package suite and passes separately; its output matches the measured candidate byte-for-byte.
- Database assertions cover stable ascending/descending pages, complete multi-Span Traces, exact totals, cross-Span conditions, resource-versus-attribute precedence, tag updates/clears, explicit nulls, exact int64/nanosecond values, literal/case-sensitive searches, invalid identity outside the current page, empty windows and explicit unsupported results. Existing budget, admission, source reconstruction, read-model invalidation and maintenance tests remain active.
- The streaming metrics regression was red before the implementation fix and green after it. The first complete Go-suite attempt had one harness failure on each engine: a source-relative maintenance-file check resolved `/work`, while the runner mounted `/signoz`. Re-running with the compile-time path mounted produced the passing results above; no product workaround was added.
- Both database cgroups recorded zero OOM events. Both database processes stopped gracefully with exit code 0; task-owned containers, database files and compilation cache were removed after preserving evidence.
- No frontend changes were made. Browser, full public HTTP deployment, production traffic, ingestion throughput, distributed CE, x86 performance, near-limit highly skewed metadata and multi-day projector residency are not qualified by this run. Passing this matrix is evidence against regressions in the covered contracts, not a proof that every workload is regression-free.

## Remaining optimization potential

| Direction | Evidence and likely benefit | Required compatibility proof before implementation |
|---|---|---|
| Increase use of existing canonical Trace read models | The raw path still processes every candidate Span's compact metadata. A current Trace projection can avoid that work and repeated source reconstruction. | Late/new Spans, revisions, tag clears, scope changes, watermark completeness and fallback must remain exact. Measure actual hit rate first. |
| Reuse selected metadata between preflight and page execution | This change reuses metadata within each statement; preflight and page execution still recompute the relation separately. | Same snapshot and exact scope/count/facet semantics; cancellation/connection cleanup; prove JSON construction cannot start before a passing budget check. Do not simply remove preflight. |
| Store a tighter encoded-size estimate | This fixture's conservative bound is 10,599,868 bytes for a 1,689,241-byte envelope, about 6.3 times larger. A tighter bound could admit more legitimate pages at the existing cap. | Versioned migration/backfill and strict readiness; escaping, Unicode, control characters and all framing/fields. Do not lower the safety factor without a proven upper bound. |
| Reduce backend object and serialization copies | JSON spooling removes a retained wire copy, but `json.load` still materializes a Python object graph; projection construction/publication also serializes documents repeatedly. | Profile real object shapes and projector lifetimes before changing parsing; preserve exact numbers, source revisions, raw attributes, exceptions and resource closure. Byte budgets are not exact process-RSS limits. |

No additional speedup or memory saving is claimed for these unimplemented directions. The original multi-day growth and 7.8 GiB/OOM incident remain unreproduced; the synthetic measurements do not establish production capacity.

## Evidence correction and reproduction

The initial report/evidence transcribed the seekdb digest with an extra `5ad` suffix. The correct reference is `oceanbase/seekdb@sha256:4db4961869d554313520d6081fca29265ba6da6db7313311bd8acbed771a956b`. The actual local image is `sha256:39a950d4caf63489638ff9aa9b7028b5dd2a896e8fc5d77ae6145fa60fd3b59d`, matching the canonical deployment pin. The tested image and historical measurements did not change; maintained report references are corrected, and historical source hashes are retained as the initial snapshot.

The Go suite uses `go test -c` with Linux Go 1.25.14, then runs the package binaries with `-test.v -test.count=1`, `SIGNOZ_TEST_OCEANBASE_DSN`, `SIGNOZ_TEST_AIVISION_BACKEND` and `SIGNOZ_TEST_NATIVE_PYTHON` against isolated fixtures. Source paths are mounted at the same `/work` used at compile time. Use `TestExportTracePageMemorySQL` from the initial report to export the production query; compare the baseline at the head above with this branch. Raw timing arrays, hashes and validation identities are in the linked JSON. All runs here are manual local evidence, separate from GitHub CI.
