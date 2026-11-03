# Trace memory fixes: merge review and integration validation

Date: 2026-10-31 (Asia/Shanghai). This validates the two memory-fix PRs combined with the main-branch additions that arrived after the [higher-load qualification](trace-query-memory-stress-20261030.md). It does not repeat or relabel that larger workload. The machine-readable [evidence](evidence/trace-query-memory-merge-review-20261031.json) records current source, runtime images, outcomes and cleanup.

## Review fixes

1. **Every shared snapshot consumer needs admission.** Canonically filtered metrics can bypass the typed aggregate endpoint and join an in-flight compact snapshot before contacting storage. The old follower path skipped its own query lease, allowing independent filtering/rendering outside the configured concurrency limit. Admit each consumer before joining the shared read. The storage owner still pays source bytes only once, admission remains idempotent, and each response retains its lease until close.
2. **The final Trace-page response needs an exact byte check.** The stored bound covers the SQL envelope, while decoding also exposes the legacy `resource` alias. Large Resource attributes could therefore produce a successful HTTP body above 24 MiB. Serialize the complete success envelope with the existing renderer's serializer, reject an oversized body with 413, and write the same serialized bytes for an accepted page. Existing STORED expressions and migrations are unchanged. Null/absence, exact integers, integer strings, tiny exponents and ordinary multi-MiB pages retain their contracts.

Both regressions were reproduced before the fixes: the Django provider/middleware test did not raise the expected busy error, and the Go handler returned 200 when 413 was required. Both pass after correction. Real CE also exercises two selected 7 MiB Resource values: stored preflight admits the SQL envelope and the final handler correctly rejects the expanded response. The existing two-Trace 6 MiB input case remains successful.

## Fixed source and combination

The final source fixes are AI Vision `22fbd23fb283f946d859030e95efcd1d69505be7` and SigNoz `438575ca76c39422d3f228c44f37787694e8e311`. They were combined without conflicts with AI Vision main `81c5b8224614ec9af4062a8ad36444ca17346ba0` (#136) and SigNoz main `b516979f4334f26a6b290c06c5de89c6bd5b8372` (#17).

Tested combination trees before this report-only addition:

- AI Vision: `6b853a6a89b234a19aaa96a58a34aa64ea014e58`.
- SigNoz: `34f9f60a968a83302f49027aa727e66c9be252bc`.

The committed combination was compared byte-for-byte with all 423 backend build-context files and 1,968 SigNoz build-relevant files: zero mismatches. The new main `spans_only` provider parameter and candidate-span paging are included. Later evidence/documentation additions do not change those application files.

## Executed checks

- Independent source reviews of both PRs found the two issues above and no further substantiated blockers after correction. They do not substitute for GitHub's required maintainer approval.
- Backend regression: one expected RED, then GREEN. Full combined-source core suite: **1,624 total, 1,576 passed, 48 environment-dependent skips**.
- SigNoz handler regression: expected RED HTTP 200, then GREEN HTTP 413. Focused real CE page suite and exact-JSON handler checks: **3 top-level tests passed**, including all page subcases.
- Full combined-source real CE packages: **211 query + 65 ingestion + 16 store = 292 top-level tests passed**. The only skipped test is `TestExportTracePageMemorySQL`, a diagnostic exporter. The backend Python differential oracle was enabled.
- Actual six-service deployment: Linux amd64, OceanBase CE 4.4.2.1, fixed supporting Daily frontend/diagnosis images and the unchanged tested gateway. Backend/SigNoz use 2 GiB caps, CE uses a 6 GiB internal budget and explicit 8 GiB test cgroup.
- Collector-ingested fixture: **500 Traces / 1,000 Spans**, 64 KiB root input, three-day window. All ten serial paths passed. C1/4/8 with two rounds produced **260 requests = 113 successful + 147 HTTP 429**, zero unexpected outcomes or assertion failures. Every successful complete response matched its serial hash; exact raw identity-set durability passed without missing, unexpected or duplicate pairs.
- Real frontend/session-authenticated filtered metrics: serial query returns exactly 500 Traces; eight concurrent queries return **2 successful + 6 HTTP 429**. Successful complete values match the filtered serial result. This checks contract/admission behavior, not a production throughput target.

The first added HTTP filter probe incorrectly used unsupported `contains`; the service correctly returned 400/code 3027. The probe was corrected to the documented source contract `LIKE`, then only that probe was rerun; the earlier matrix/durability results remain unchanged. The cleanup guard initially treated Docker's nondeterministic mount-list ordering as a change; comparing sorted mount definitions confirmed the original resources had not changed. Failed attempts remain in local receipts.

## Reproduction and evidence scope

The private VM review directory is `/data/0/aiv-memory-merge-review-20261031`. Backend checks ran `manage.py test apps.core.tests --noinput` with the combined source mounted read-only in the locked Python runtime. SigNoz packages were compiled with `go test -c` from the combined source and run with `-test.v -test.timeout=20m -test.run .`, an isolated real-CE DSN, `SIGNOZ_TEST_AIVISION_BACKEND=/app` and `SIGNOZ_TEST_NATIVE_PYTHON=/app/.venv/bin/python`. Logs and full source manifests are retained privately; public evidence contains hashes and sanitized receipts.

Application API probes use the real frontend proxy and session/public authentication; direct SigNoz probes use its API key. The writer traverses the collector. The frontend is a fixed supporting Daily image; browser rendering and a rebuild of new main's frontend are not claimed. These are manually executed source-runtime checks, not hosted CI or customer-release artifact qualification.

The new namespace's six containers stopped normally, with exit zero and no OOM; its two volumes and one network were removed. All 42 original containers, including the 13 originally running, retained identity, image, start state, memory limits and mount definitions. The earlier private seekdb failure archive was not touched.

The higher-load 413/429 limitations, original multi-day memory growth/7.8 GiB OOM, fixed seekdb kernel and affected-table recovery remain unqualified as documented in the prior report and AI Vision DB-002. No customer deployment, image publication or release is included in this source review. Hosted CI availability and GitHub approval/merge status are checked separately at the actual PR head.
