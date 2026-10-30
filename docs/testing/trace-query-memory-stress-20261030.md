# Higher-load VM trace memory and regression validation

Test date: 2026-10-30 (UTC). This extends the [earlier VM qualification](trace-query-memory-vm-20261030.md) using larger datasets and more concurrent requests. OceanBase CE reports `5.7.25-OceanBase_CE-v4.4.2.1`. The engines run separately on the same prepared Linux amd64 VM. Current-source qualification prioritizes OceanBase CE; seekdb higher-load observations are historical and its blocked helper upgrade is recorded separately. This is source-runtime qualification, not a release-image or production-capacity claim.

## Additional failures found and corrected

1. **Gateway memory refusal:** the first 6,000-Trace seekdb seed stopped after 3,765 acknowledged Traces on HTTP 500, while the collector logged memory-limiter refusals. Its vendored receiver had already classified the error as `Unavailable`, but the HTTP adapter discarded that classification. All six traces/logs/metrics handlers now return 503 and `Retry-After: 1` for that typed error. Authentication, malformed requests, permanent errors, encodings and successful responses retain their contracts. This follows the [OTLP retryable-response contract](https://opentelemetry.io/docs/specs/otlp/#retryable-response-codes). The interrupted fixture was repaired and reconciled before query testing; its resumed acknowledgement time is not full-dataset ingestion throughput.
2. **SigNoz window admission:** the 512 MiB source-window guard produced an ordinary error, so callers could receive 400 or a generic upstream failure. A distinct resource-budget sentinel now maps to 413. It remains separate from the representation-size sentinel and cannot trigger the raw-representation retry. None of the byte, Span, Trace or admission limits increased.
3. **Framework dashboard adapters:** V2 converted native 3036/3037 errors to generic 500. Single queries now retain 413/429 with structured error types and the original native code. Batch queries retain their HTTP 200 envelope, successful siblings and per-item error status; unrelated errors keep their earlier classification.

4. **Metrics-window SQL timeout:** the corrected seekdb soak returned one unexpected public Trace-page 503. SigNoz logged database error 4012 at the unchanged ten-second query timeout while reading the metrics recovery window. That path still transferred each complete canonical JSON document before discarding input/output. Two additional STORED helpers retain metrics observations and original field presence; existing stored metadata and operation previews reconstruct the exact compact representation. Missing, ordinary, virtual or conflicting helpers retain the previous bounded stream. Null versus absence, literal dotted keys, number types, snapshots, physical authorization, revision and source/wire limits remain checked. The failed soak remains failed evidence.
5. **Optional migration blocked service startup:** provider startup and the managed ingest router both attempted optional helper migration. Slow or blocked DDL could therefore delay the API listener. The mandatory eligibility fence remains synchronous; optional helpers move to one provider-owned cancellable worker, and the managed router no longer repeats them. A 15-minute client/session budget applies only to summary migration; ordinary SQL stays at ten seconds. A 20-minute lease coordinates attempts but does not prove remote DDL termination after expiry. The 11,501-row seekdb failure was subsequently diagnosed as a pre-task table-lock conflict, not measured backfill cost. The application change does not repair the engine lock state.

The gateway and V2 adapter changes belong to AI Vision; the storage, query and migration changes belong to SigNoz.

During CE burst preparation, existing ingestion and projection publication serialized on the global maintenance row. A retained transaction held its exclusive fence for approximately 7.2 seconds while other requests waited; retryable 1205 errors occurred. Raw ingestion subsequently drained without collector drop messages, and projection counts continued increasing. This was active transaction contention, not a demonstrated orphan lock or a new-patch regression. Fixture readiness timings include collector retries and projection catch-up; they must not be presented as isolated ingestion throughput. Replacing that fence with a shared lock is not part of this patch because OFF, generation changes and ingestion invalidation need the same atomic boundary. Both repositories include this shared integration evidence. No customer deployment, release tag or image publication changed.

## Tested source identity

The VM used dirty source overlays on AI Vision `0d7513f44a4ad80b0e136f0811919cf6e4c05ebf` and SigNoz `bd4325c2bff4325167823dd50e45773a758854bc`. After validation, their exact source bytes were committed as AI Vision **`42216024fb19b43f3b8f8492d0cc7f70f590db0b`** and SigNoz **`ec0e59473cd3030fdeb0f9f6e01194de4b6da8e8`**. The committed three-file backend overlay, complete 3,567-file gateway build context and 13-file SigNoz overlay were compared to the frozen VM manifests with zero mismatches. Subsequent report commits do not change these application sources.

| Component | Final source-runtime image ID |
|---|---|
| Backend | `sha256:956f782fa7c73c38aca0817f59c903ca4ab7549d20ecf42f7398bca4c8fca717` |
| Gateway | `sha256:0ecd89e98d8c9648646551ca1dbe8f69bf558e7795aeae1a68ae02eba37fddfb` |
| SigNoz | `sha256:c714dc72a7a633933e5a80a2e17a28b0f6395d91cdb8d377923fad70d372ed46` |
| OceanBase CE | `sha256:9c850a9ce79c79e9c30c347388318662afd11b354a0893f56ceabfd93203ce28` |

## Workload and assertions

The prepared VM has four vCPUs, 30,529,953,792 bytes RAM and no swap. Backend and SigNoz retain 2 GiB cgroups. seekdb retains a 4 GiB cgroup and `memory_budget=2G`; CE retains `memory_limit=6G` and an explicit 8 GiB test cgroup. Only test disk allowances increased. Earlier runtime SQL confirms seekdb 1.4.0.0 treats `memory_limit` as an ignored compatibility parameter; `memory_budget` sizes caches and buffers, while the cgroup provides a separate container bound.

Three isolated personal Spaces use a fixed three-day window, two Spans per Trace and ten Traces per Session. Inputs deliberately contain repetitive synthetic text, so database compression and IO are not representative of production payload entropy:

| Read fixture | Traces / Spans | Root input | Page size | Purpose |
|---|---:|---:|---:|---|
| main | 6,000 / 12,000 | 64 KiB | 50 | Four times the previous Trace count; deliberately above whole-window source admission |
| control | 3,000 / 6,000 | 64 KiB | 50 | All ten endpoints within source admission |
| large | 500 / 1,000 | 256 KiB | 10 | Four times the previous per-Trace input; complete selected content |

Each fixture executes ten serial probes and two rounds per path at concurrency 1, 8, 16 and 32: 1,140 load attempts per fixture and 3,420 per engine. Successful load responses must match their same-fixture serial content hash, excluding only declared elapsed-time diagnostics. Failed requests do not contribute to successful latency. The clients deliberately do not hide 429 by retrying reads.

Independent assertions cover exact order/totals, all source text, Unicode/control characters, explicit null, integer precision, tags, Session membership, token totals, model/provider groups, every time bucket and physical Space/user isolation. Additional probes cover deep/last/one-past-end/repeated pages, three selected details, three Sessions and cost-free typed aggregates. The main fixture's cost-free aggregates must actually use `statistics_aggregate`. All raw `(trace_id, span_id)` pairs are compared with the complete generated set, including missing, unexpected and duplicate checks; gateway acknowledgements alone do not qualify durability.

The final continuous workload offers eight closed-loop readers for 600 seconds across 27 budget-safe case/path combinations, with a minimum 250 ms interval per reader. A separate Space concurrently writes 1,000 new 64 KiB Traces in one-Trace OTLP batches. Requests started before the deadline retain their ordinary timeout and drain afterward. Writer completion, projection catch-up and 60 seconds idle are measured separately. Four container cgroups are sampled every five seconds; kernel lifetime peaks and OOM counters are retained in the JSON.

## Results

Current-source OceanBase CE results (successes and explicit refusals are separate):

| Fixture | Load attempts | HTTP 200 | HTTP 413 | HTTP 429 | Hash / unexpected / assertion failures |
|---|---:|---:|---:|---:|---|
| main | 1140 | 222 | 189 | 729 | 0 / 0 / 0 |
| control | 1140 | 323 | 0 | 817 | 0 / 0 / 0 |
| large | 1140 | 330 | 0 | 810 | 0 / 0 / 0 |

Each serial contract is enforced independently: main requires seven successes and three explicit source-budget 413 responses; control and large require ten successful paths. Concurrent successes must match the complete corresponding serial hash.

Five-second cgroup usage samples during each matrix (MiB, including cgroup cache; not process RSS):

| Fixture | Backend peak | SigNoz peak | CE peak | Gateway peak |
|---|---:|---:|---:|---:|
| main | 337.66 | 260.67 | 5806.90 | 77.57 |
| control | 490.31 | 482.74 | 5874.50 | 28.78 |
| large | 486.58 | 244.18 | 5919.00 | 28.78 |

Continuous workload results are recorded in the machine-readable evidence under `engines.oceanbase.mixed-soak-metrics`; all request outcomes and memory phases retain their original classification.

The 600-second reader offer completed **9,996 reads: 1,015 successes, 8,981 HTTP 429 responses and 0 unexpected failures**. All 27 selected paths produced successes matching serial hashes. The independent writer acknowledged 1,000 requests and persisted the exact 1,000 Traces / 2,000 Spans. Readers drained at 602.938 seconds; the writer completed at 635.759 seconds, followed by 31.164 seconds of projection catch-up and 60.001 seconds idle.

| Mixed workload cgroup | Start MiB | Sampled peak MiB | End after idle MiB | OOM kills / allocation failures |
|---|---:|---:|---:|---|
| Backend | 470.91 | 573.05 | 527.62 | 0 / 0 |
| SigNoz | 159.55 | 468.80 | 121.06 | 0 / 0 |
| OceanBase CE | 5691.11 | 5939.31 | 5872.45 | 0 / 0 |
| Gateway | 29.23 | 41.04 | 39.04 | 0 / 0 |

#### Same-fixture metrics SQL profile

For the same 3,000-Trace control fixture, the projected-fetch stage transferred **416,300,794 bytes → 19,308,262 bytes** (95.36% fewer decoded SQL field bytes) and completed in **3.409 s → 0.225 s**. These are one legacy and one stored-helper read on the same prepared CE instance, not repeated production benchmarks. Each path used four independent read-only repeatable-read SQL snapshots and the unchanged ten-second session timeout. Timings include Python row streaming/hashing and exclude Go decoding/HTTP encoding; the representations intentionally have different raw hashes. Exact resulting metrics semantics are checked by the application regression suites and HTTP fixtures. No SQL-transfer percentage is presented as a process-memory reduction.

#### Logical stored-helper footprint

| Fixture | Canonical row JSON bytes | New two helpers bytes | New / canonical | All six helpers bytes |
|---|---:|---:|---:|---:|
| main | 832,187,050 | 17,910,708 | 2.15% | 855,973,758 |
| control | 416,090,794 | 8,955,372 | 2.15% | 427,984,166 |
| large | 265,954,370 | 1,492,580 | 0.56% | 267,936,616 |

These are logical UTF-8 column-byte sums, not compressed disk allocation or an atomic multi-table snapshot. The previous content helper duplicates much of canonical input/output; the two new metrics helpers add the smaller amount shown separately. Generated expressions still execute on writes. The read reduction must be weighed against storage, backfill and ingestion cost; production ingestion overhead is not qualified. The optional `performance_schema.metadata_locks` diagnostic view was absent (1146); readiness, definitions and byte-sum checks completed independently.

The main fixture contains approximately 775 MiB across the three raw JSON columns; the native admission estimate also includes other source fields and is larger. Input occurs in more than one stored field. Its Session detail, default cost-inclusive dashboard and public Trace list exceed the existing 512 MiB whole-window guard and return explicit 413, or 429 when admission is busy. Those rejections are not successful functional results. Selected Trace pages/details, Session lists and typed cost-free aggregates remain available. The control and large fixtures qualify all ten serial paths.

The relatively high 429 count demonstrates the configured admission limit under a deliberately aggressive client workload. These results do not establish that 32 users can all receive immediate successful responses, and fast rejection is not counted as a latency improvement. Full-window fallback paths remain substantially more expensive than selected read-model pages.

## Fault injection and regression suites

On the exact frozen CE source images, all 78 edge probes and nine isolation probes passed, and all 9,500 fixture Traces / 19,000 Spans matched the complete raw identity set. No duplicates, missing or unexpected pairs were found. Encrypted V2 normal mode passed nine requests. The corrected maintenance-OFF-only run passed nine requests, including eight resource-error items and exact small-window event/timeseries values; status aliases preserve the existing contract described below.

The real-CE Go binaries passed 207 AI Vision storage/query tests, 65 ingestion tests and 16 telemetry-store tests: **288 passed**, with one skipped diagnostic SQL exporter (`TestExportTracePageMemorySQL`), not an unexecuted behavior test. Python equivalence checks were enabled in the candidate backend image. The local backend core fixture suite passed 1,482 tests with 48 environment-dependent skips. The local gateway suite passed 22 top-level tests with two environment skips; its receiver matrix covers 96 leaf cases. These local results and their skips remain separate from real VM evidence.

A direct retained-data rebuild on the original complete CE test table passed for **10,501 rows / 1,653,100,239 bytes of canonical JSON** (read fixtures, writer and gateway-replay data). The same two-column ADD completed in **277.462 seconds** under the dedicated 900-second migration session budget; the complete before/after verification took 332.154 seconds. Exact identity membership, every selected base/generated column, canonical JSON and the new helpers have equal before/after hashes. Column definitions and index definitions match; physical table/index IDs and column ordinal positions need not remain identical after a rebuild. Durable OFF, generation and other latch fields are unchanged, and the ordinary ten-second SQL timeout was preserved/restored. Engine diagnostics record a completed `DDL_TABLE_REDEFINITION` for all 10,501 rows, with three associated index builds. This is actual backfill work, distinct from seekdb's pre-task lock conflict.

Three clone-copy preparation attempts hit database timeout 4012 before the target two-column ALTER started: the first full-schema batch copy, its smaller-batch continuation in the larger-input fixture, and a base-column-only clone. The smaller-batch copy was intentionally checkpointed at 6,000 progress to separate fixture preparation from the full migration allowance. All failed/interrupted receipts remain, and their scratch tables were removed. Since even the base-only copy failed, existing generated helpers alone are not established as the cause. These synthetic `INSERT ... SELECT` preparation failures are outside the 32-step query qualification and are not relabeled passes.

The final direct rebuild avoids cloning entirely. It operates only on this disposable synthetic CE database after all application services have stopped: hash every original row and column, drop exactly the two rebuildable optional helpers, run the same two-column ADD on the existing complete table, and compare every row/helper plus column and index definitions. Maintenance OFF remains unchanged. This is a real full-schema retained-data DDL check; it does not qualify live-write contention, production migration sizing or a fixed seekdb kernel. The source-runtime migration/lifecycle suite separately covers the application control flow.

A separate collector container, using the same immutable candidate image, credentials and pipeline, temporarily sets only the memory limiter to 1 MiB to force a real refusal. The existing gateway is untouched. The probe requires 503, `Retry-After: 1`, OTLP status code 14 and zero stored Spans; it then recreates only the probe at the normal 512 MiB setting, replays the same IDs and verifies exact durability. This is explicit fault injection, not a production-limit throughput measurement.

Encrypted V2 HTTP probes use the deployment's actual AES-GCM configuration, with keys kept inside the backend container. They check normal full-window, 24-hour and 50-Trace results. After all normal workloads and database suites, the test-only maintenance latch is switched OFF through its fenced sticky-OFF transaction. Optimized reads must fall back, preserve 413/3036 for oversized windows and retain successful small-window results and batch structure. The latch is not directly flipped ON; that isolated database is removed during cleanup. A retained deployment would require the documented resume/rebuild operation.

## seekdb remains a known engine issue

The historical higher-load candidate predates the new compact metrics helpers. Its 3,420 attempts produced 902 successful results, 188 HTTP 413 and 2,330 HTTP 429 responses, with zero successful-response hash mismatches or unexpected matrix errors. Exact raw identity sets also passed. Both subsequent continuous runs failed, as retained below; no final metrics-optimization qualification is claimed on seekdb.

On the affected 11,501-row standalone projection table, adding the two STORED columns repeatedly failed to acquire the table lock before creating a DDL task (`task_id=0`, `hidden_table_id=-1`). A retained 72.607-second log window contained 697 retries: the global RUNTIME lock succeeded, TABLE 500031 returned `-6003`, and ALTER returned `-4023`. The corresponding table/tablet had orphan `OUT_TRANS_UNLOCK / COMPLETE` records without matching LOCK entries. This matches the signature discussed in [seekdb #2207](https://github.com/oceanbase/seekdb/issues/2207), but the earlier checkpoint/restart sequence was not reconstructed. The wait is not demonstrated large-table backfill cost.

Same-schema 0/1/100/1,000-row controls completed the same ALTER in 1.476/1.333/2.944/19.242 seconds. A separate 1,000-row ALTER canceled after 3.020 seconds succeeded on retry in 35.221 seconds. These controls neither recover the original table nor establish cancellation as a sufficient trigger. No OOM or disk-exhaustion evidence explained the observed block. The repaired kernel in [PR #2183](https://github.com/oceanbase/seekdb/pull/2183) remains unmerged at this report's check and has not been run here.

The optional-startup candidate became available in 46.682 seconds and passed 20 selected edge probes plus three isolation probes with the new helpers pending. Its 1,200-second readiness observation failed. The previous bounded metrics stream remains available but retains its higher query cost; no general user-operable workaround is verified. The [standalone diagnostics and controls were posted upstream](https://github.com/oceanbase/seekdb/issues/2207#issuecomment-6523763176) and recorded in AI Vision `KNOWN_ISSUES.md` as DB-002. A private, stopped-volume archive preserves the affected database for later fixed-kernel verification; only its hashes and validation receipt are public.

## Evidence boundaries and cleanup

- The first seekdb continuous run has eight client timeouts at 600.002–600.036 seconds because the original harness shortened in-flight request timeouts to the remaining offered-load interval. It remains failed evidence. The fixed harness is regression-tested with a slow request crossing the deadline, and the complete 600-second run uses a new independent writer Space. No old timeout is relabeled successful.
- The corrected second seekdb soak had one real SQL timeout among 7,110 reads (682 successes, 6,427 admission refusals and one unexpected 503). Its new writer still persisted all 1,000 Traces / 2,000 Spans. The timeout triggered the metrics-window correction above and is not counted as acceptable admission or relabeled successful.
- The first encrypted V2 probe had a fixture-oracle error: canonical preview content contains one explicit message wrapper. The corrected oracle unwraps only that exact shape and then asserts the full synthetic payload; six focused oracle regressions pass. Product content did not change.
- An early `public_exact_tags` oracle expected the child's tags. The root ends later and supplies Trace tags, so the completion-order contract selects the root. The oracle was corrected against the actual projection implementation before final matrices; product tag behavior did not change.
- The first final CE maintenance-OFF assertion rejected symbolic `OK`/`ERROR`, although the existing raw fallback returns these and the optimized preview returns numeric `1`/`2`. All 50 row differences were isolated to this pre-existing representation. The relevant product files and preview logic were unchanged from the PR base. A separate seven-test oracle accepts only the established aliases and rejects reversed, unknown, boolean and float statuses. A new OFF-only run passed without mutating the durable latch. The original failed receipt remains unchanged; a hash-bound resolution receipt records the two completed final steps and unchanged application source/images.
- All fixture data is synthetic. Application API probes use the real frontend proxy and session/public authentication through the backend. The direct raw/read-model probes connect to SigNoz with its API key; they do not traverse the frontend or collector. Writer and refusal/replay probes traverse the collector, with durability checked in the database. No browser rendering or real external SDK compatibility is inferred.
- The original multi-day growth and 7.8 GiB OOM are not reproduced or ruled out by a ten-minute test. Five-second samples can miss short peaks; lifetime cgroup peaks have a wider scope than an individual query. No general RSS reduction percentage or cross-engine performance ranking is claimed.
- Earlier source/runtime results remain historical. Current frozen source hashes, parent image identities, per-path counts/hashes/timings, memory samples and cleanup receipts are in [the machine-readable evidence](evidence/trace-query-memory-stress-20261030.json).

Cleanup removed only the task-owned 12 containers, three named volumes and two networks after graceful stops. All 42 original containers (including their 13 running states), 11 original volumes and ten original networks remained unchanged. The failed seekdb database survives in a private verified offline archive: 193,807,226 bytes, SHA256 `43ed3d9659e68b0e4bd4aac16b7b3ae3b138f11ca1032633a8528e8604fc8c07`; archive readability, all 23 file hashes and unchanged source manifests were checked before cleanup and the archive hash was checked again afterward. No raw database archive or credentials are committed.

The manual VM harness is retained under `/data/0/aiv-memory-stress-20261030`; its source hashes, step plan, immutable image/source manifests and receipts are recorded in the JSON. It is a prepared-environment harness and is not a general deployment command. The executed entry points were:

```text
python3 final_qualify.py --execute --engines oceanbase
python3 complete_ce_qualification.py
python3 restart_ce_for_backfill.py
python3 ce_retained_backfill.py --execute  # failed before target ALTER
python3 ce_retained_backfill_verified.py --execute  # preparation attempt, intentionally checkpointed
python3 checkpoint_ce_backfill_copy.py
python3 ce_retained_backfill_resume.py --execute  # larger-copy preparation timed out
python3 ce_retained_backfill_baseclone.py --execute  # copy preparation timed out
python3 ce_retained_direct.py
python3 stop_ce_after_backfill.py
python3 cleanup_vm.py --execute
```

The first qualifier retains its failed final OFF-oracle receipt; `complete_ce_qualification.py` verifies that exact failure, unchanged source/images and original passed steps before running the independent OFF-only correction and graceful stop. Its completion receipt covers all 32 planned steps without rewriting the original result. The later database-only restart/backfill is outside that lifecycle. The six CE services retained their original start times throughout qualification and were stopped without OOM.

For real CE package tests, the harness compiled `go test -c` binaries for `./pkg/modules/aivision`, `./pkg/ingest` and `./pkg/telemetrystore/oceanbasetelemetrystore` from the frozen SigNoz source, then ran each with `-test.v -test.timeout=20m -test.run .` in the frozen backend image. `SIGNOZ_TEST_OCEANBASE_DSN` was injected privately; `SIGNOZ_TEST_AIVISION_BACKEND=/app` and `SIGNOZ_TEST_NATIVE_PYTHON=/app/.venv/bin/python` enabled the Python oracle. Temporary test containers were removed. The report and JSON were checked against actual private credential values before being committed.
