# Trace memory follow-up: full-stack Linux VM validation

Date: 2026-10-30. This extends the [initial memory retest](trace-query-memory-20261030.md) and [SQL follow-up](trace-query-memory-followup-20261030.md). [Machine-readable evidence](evidence/trace-query-memory-vm-20261030.json) includes source-file hashes, image identities, response hashes, successful latency arrays, refusal counts, memory samples, and validation results. These are manual runs, separate from hosted CI.

## Problems found and fixed

| Area | Failure or unnecessary work | Implemented correction |
|---|---|---|
| AI Vision projector | Admission ended after fetching, while decoded sources and publication payloads remained live; repeated JSON serialization created redundant copies. | Hold admission through projection/publication, close generators and release references on failure/cancellation, and reuse exact measured JSON envelope sizes. |
| Session details and public Trace pages | Reading a small selected result could load the entire three-day raw window and exhaust the request budget. | Select from compact metrics facts, then hydrate only selected complete Traces. Fence source revision, maintenance generation, physical scope and complete Span identities. Preserve any-observation Session membership and canonical Session semantics. |
| Capacity error adapters | Public and unified routes translated known capacity errors into generic 503/400 responses. | Preserve explicit 413 byte-budget and 429 admission responses, with Retry-After for admission. |
| SigNoz canonical lists and recovery | JSON extraction/removal from complete canonical rows ran before selected-page preflight; dirty enrollment parsed full rows; operation pages repeatedly decoded complete preview maps. | Persist versioned metadata/content/version/preview helpers, preflight stored byte sizes, read each selected Trace's preview map once, and retain only selected operation previews. Add a per-row recovery guard for oversized historical canonical rows. |
| Raw Trace page admission | A six-times source estimate rejected valid pages, including a two-Trace, 6 MiB input fixture. | Persist an encoded-wire upper bound that accounts for JSON, HTML, Unicode/control escaping and framing. Keep the existing 24 MiB cap, exact snapshot, metadata preflight and admission guard. |
| OTLP ingestion under contention | OB CE returned MySQL 1205; SigNoz returned HTTP 500, which the actual collector treated as non-retryable and dropped. | Map typed lock timeout/deadlock errors (1205/1213, including wrapped errors) to HTTP 503 with Retry-After on both traces and logs. Unknown/permanent/ambiguous failures retain their prior status. |
| Upgrade | Four independent stored-column ALTERs took 136.6 seconds before SigNoz listened and exceeded deployment readiness. | Validate all definitions under the migration lease and add missing independent columns in one ALTER. The same historical dataset then reached API listen in 55.2 seconds; full deployment passed in 118.6 seconds. |

The ingestion correction follows [OTLP retry semantics](https://opentelemetry.io/docs/specs/otlp/#retryable-response-codes). Older publishers still populate STORED helpers automatically. Conflicting or missing helper definitions cannot establish readiness; query paths decline the capability and retain bounded compatibility behavior. The durable maintenance OFF latch remains intact. The projection context moves to `native-trace-summary-pricing-v7-session-membership`, so old rows are rebuilt before using new membership facts.

## Environment and procedure

The prepared VM was Linux x86_64, 4 vCPU, 30,529,953,792 bytes RAM, no swap, Docker 26.1.3. Each engine ran separately in its own six-service deployment: database, SigNoz, collector gateway, AI Vision backend, AgentLens and frontend. Thirteen unrelated containers were preserved. Backend and SigNoz each had a 2 GiB cgroup; seekdb used a 4 GiB cgroup and `memory_budget=2G`; CE used `memory_limit=6G` and an explicit 8 GiB test cgroup applied with `docker update`. That CE cap is a test setting, not a new canonical CE deployment feature.

Databases were pinned to seekdb `4db4961869d554313520d6081fca29265ba6da6db7313311bd8acbed771a956b` and CE `eaa2c3b3513fa21e4a658f197c1deb93c9d6b6eb742cfa5ef0f085f82b9cec54` manifest digests. Actual amd64 image IDs are in the evidence. Supporting services use fixed `0.0.1-daily.20261028.2` image IDs. Backend images use the locked Python 3.11.14 runtime and replace application source; SigNoz is compiled with Go 1.25.7 linux/amd64. This qualifies the tested source/runtime combination, not a freshly published native Daily artifact. No release tag or deployed customer image was changed.

Baselines were AI Vision `adfa28c2c305970190aef7de8636dfce7eb263ea` and SigNoz `f5c2e26fc8e2f3524c86229358e4666b10a9a304`, already containing the earlier memory optimizations. Candidate overlays comprise the 16 backend and 36 SigNoz files identified by SHA-256 in the evidence. Report-only additions do not change those tested sources.

The harness registered synthetic accounts, created a personal Space/Agent and SpaceKey, obtained an ingest JWT, and sent OTLP through the real collector. It seeded 1,500 Traces / 3,000 Spans / 150 Sessions with 64 KiB root input over a fixed three-day window. Both phases reused the same persisted per-engine fixture. Data includes Unicode, controls, explicit null, exact int64 values, root/child attributes and complete multi-Span Traces. Requests use the frontend HTTP proxy, session authentication or public Basic authentication; direct SigNoz probes use its scoped API key.

Ten paths cover first/last Trace pages, Trace detail, Session list/detail, dashboard metrics, public Trace detail/page, direct raw page and direct canonical page. Each phase executes one serial probe per path and two rounds at concurrency 1, 2, 4 and 8: 300 load attempts. Additional tests cover outsider/Agent/Space isolation, valid maximum-page rejection, three minutes of mixed reads plus 40 seconds idle, and a forced 20-second ingestion lock. Engines were not compared as a performance ranking.

The first CE seed burst was incomplete: only 435 Traces / 870 Spans persisted, and collector logs accounted for 2,130 dropped Spans after non-retryable 500 responses. This failed attempt is excluded from performance comparisons. Missing deterministic Traces were replayed in persisted batches; only the verified 1,500/3,000 fixture was used for the CE baseline. Candidate retry probes use a separate synthetic account after query measurements.

## Results

All ten serial paths succeed on both candidate deployments, versus seven on each baseline. No candidate functional assertion, unexpected-error or isolation check failed. Candidate load totals are **162 successes + 138 HTTP 429 refusals on seekdb**, and **157 successes + 143 HTTP 429 refusals on CE**. Fast refusals are excluded from successful latency. These small per-path samples do not establish production P95 or throughput.

Successful C1 medians in milliseconds (two observations per successful path):

| Path | seekdb baseline | seekdb candidate | CE baseline | CE candidate |
|---|---:|---:|---:|---:|
| trace_first | 543.18 | 116.48 | 755.65 | 182.23 |
| trace_last | 532.44 | 137.49 | 722.88 | 185.42 |
| trace_detail | 71.23 | 72.47 | 88.25 | 105.78 |
| sessions | 3,443.99 | 187.02 | 4,836.81 | 262.72 |
| session_detail | — | 3,100.55 | — | 3,582.80 |
| dashboard_metrics | 3,772.26 | 3,718.22 | 4,164.90 | 4,233.54 |
| public_detail | 116.26 | 94.29 | 117.89 | 127.46 |
| public_trace_page | — | 3,615.66 | — | 3,840.76 |
| direct_raw_page | — | 756.89 | — | 775.88 |
| direct_read_model | 524.74 | 55.52 | 652.33 | 114.63 |

A dash means there was no successful baseline response, not zero latency. Dashboard medians remain similar; the isolated CE serial sample rose 4.41→6.90 seconds but that increase did not repeat in the C1/C2/C4 groups. No universal speedup is claimed.

Candidate mixed-read soak: two callers for 180 seconds, then 40 seconds idle. Every request succeeded: **200 on seekdb, 168 on CE**. Backend sampled cgroup usage stayed near its starting plateau:

| Engine | BE start | BE sampled maximum | BE after idle |
|---|---:|---:|---:|
| seekdb | 384.71 MiB | 390.33 MiB | 385.00 MiB |
| OB CE | 377.96 MiB | 403.90 MiB | 378.23 MiB |

No OOM kill was recorded in either engine’s workload, soak or retry samples. A three-minute plateau does not establish multi-day memory stability. Full query-phase memory samples, including SigNoz and database cache variation, are in the JSON; they do not support a blanket reduction percentage.

After a forced 20-second maintenance-row lock, both real collector pipelines persisted all 1,500 new Traces / 3,000 Spans with **zero drop messages**. seekdb recorded 8 retryable 503 responses and reached persistence in 55.54 seconds; CE recorded 16 and took 169.74 seconds. Gateway acknowledgement occurred earlier (5.20/5.61 seconds) and is not treated as durability. This checks raw ingestion durability, not immediate projection completion or throughput.

Supplemental valid raw-page probes on each engine returned **200 with all 6,873,155 response bytes at limit 50**, and **413 at the legal maximum limit 200**. The main scoped fixture still contained exactly 1,500 Traces / 3,000 Spans, 1,500 current projections and zero dirty sources.

## Functional and resource validation

- The latest backend core suite ran 1,527 tests: **1,479 passed, 48 skipped** (29 unavailable database fixtures and 19 opt-in E2E). Unit HTTP dependencies are fixtures; the VM workload supplies separate real-service evidence.
- SigNoz on real seekdb, final sources: **201 query tests, 61 ingestion tests and 13 store tests passed**, with one opt-in SQL-export helper skipped. On CE: **201 query, 60 ingestion and 13 store tests passed** before the final combined-ALTER change; all **5 affected ingestion migration tests passed again** on the rebuilt final sources, including actual historical backfill. The final full stack also exercised that migration on both engines.
- Real-database cases cover historical helper backfill and old-writer updates, strict stored-expression readiness, explicit nulls, case-sensitive scope changes, exact int64/nanosecond values, stable complete Trace pages, Session aggregation, operation previews, maintenance concurrency, recovery and the independent Python raw-page oracle.
- Independent selected-source review checked seven malformed/auth/time-boundary cases and twelve tied timestamp/id-page cases against full canonical results. Cancellation and projector-lifetime regressions were red before correction and green afterward.
- On CE, seven serial paths with successful baselines preserve response semantics: three primary hashes match, and four secondary hashes match after the explicitly audited projection-context upgrade. All 122 recorded read-model contexts match their independently runtime-asserted same-phase anchors. On seekdb, three primary hashes match; first/last Trace, Session and direct canonical pages preserve ordered IDs/totals, but the old baseline lacks secondary hashes/context recording, so complete cross-phase equality is not claimed for those four paths. All successful concurrent responses match their same-phase serial reference hashes (seekdb baseline/candidate 108/162; CE 107/157). All 608 seekdb and 573 CE candidate recorded assertions pass; dashboard has whole-response comparison but no independent fixture-value assertion.
- The size-check profile retained the same 15,394,085-byte envelope. Reusing exact envelope sizes reduced measured Python traced allocation peak from 30,788,244 to 8,358,282 bytes and size-check median from 96.50 to 46.34 ms. This is a local serialization microbenchmark, not process RSS or network transport memory.
- All task application/database containers were stopped gracefully with exit code 0 and no OOM. Twelve task containers, three owned volumes and two owned networks were removed. All original containers retained their IDs, image references and running states; original volumes/networks were preserved. Source and evidence artifacts remain available for reproduction.

The first Go VM attempt had two oversized test INSERTs exceeding CE's 16 MiB packet limit. Fixtures were split or reduced without changing production limits; valid SQL aggregation above 16 MiB was separately confirmed. The original oversized-page harness requested invalid `limit=1000` and returned 400; those receipts are preserved as invalid capacity evidence. The supplemental valid `limit=200` test is recorded separately. No passed result is inferred from either failed harness attempt.

## Tradeoffs and unverified scope

seekdb: 1,500 canonical rows total 208,015,666 bytes; the four helpers total 209,480,166 additional logical bytes (100.70% of canonical row JSON). oceanbase: 1,500 canonical rows total 208,015,666 bytes; the four helpers total 209,480,166 additional logical bytes (100.70% of canonical row JSON). These logical byte sums exclude indexes, compression and physical storage allocation; they are not a whole-database growth percentage.

Generated helper values shift JSON processing to publication/backfill and add storage/write work. Large deployments need a maintenance window and free capacity; the 55.2-second migration is a 1,500-row fixture observation, not a production sizing estimate. Ingestion retry restores durability in the tested contention scenario but its recovery latency is visible in the evidence; this is not an ingestion throughput claim.

Memory values distinguish cgroup usage (including cache), working set (`usage - inactive_file`), process RSS and lifetime high-water marks. Samples can miss transient peaks; startup/migration and earlier work contribute to lifetime peaks. Baseline/candidate processes and successful/refused request mixes differ. The seekdb baseline has only one valid host-side post-load sample, so it cannot support a comparable before/after memory curve. No blanket RSS-reduction percentage is claimed.

No UI code changed; frontend HTTP proxy checks are not browser-rendering proof. Production traffic/capacity, distributed CE, external SDK interoperability, multi-day memory residency and the original 7.8 GiB/OOM event remain unqualified. Simple public Trace pages within one retained-history window are optimized; legacy filters/orders and multi-window history retain bounded compatibility scans. The compact metrics recovery path still streams complete canonical rows one at a time before dropping IO, so dashboard work is not eliminated. Raw legacy query contracts outside the tested native paths are not claimed covered. Admission is per worker and additional workers multiply capacity. Byte budgets are not exact RSS limits.

## Reproduction

Use the recorded source baselines plus the candidate files whose hashes are attached. Deploy each engine separately using the canonical `aiv cast` lifecycle with the fixed image identities and limits above. Seed once, verify exact persisted Span/Trace counts and projection readiness, then reuse the dataset across baseline and candidate. Do not include incomplete ingestion attempts in query comparisons. Authenticate through the normal product routes and run the ten paths/concurrency matrix above; record complete response hashes and independent identity/count/value assertions as well as errors.

For backend source checks, run `NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost uv run python manage.py test apps.core.tests --noinput` from `ai-vision/aivision-py-be`. For SigNoz, compile/run the `pkg/modules/aivision`, `pkg/ingest` and `pkg/telemetrystore/oceanbasetelemetrystore` test packages with the recorded Go version and isolated `SIGNOZ_TEST_OCEANBASE_DSN`; enable `SIGNOZ_TEST_AIVISION_BACKEND` and `SIGNOZ_TEST_NATIVE_PYTHON` for the Python oracle. Mount source at the same `/src` compile-time path for source-relative lifecycle assertions. These are manual validation steps, not evidence that hosted checks executed.
