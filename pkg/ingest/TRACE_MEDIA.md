# Trace media data plane

SigNoz owns media storage, source reconstruction, associations, authorization,
quota and retention. AI Vision authorizes the platform user or SpaceKey and
proxies these APIs; it does not query media tables.

## Ingestion and representation

The shared OTLP span transaction recognizes explicitly typed media in known
input/output attributes. It stores decoded bytes and exact original attribute
strings before inserting display references. A failed write rolls back the span
and media together, allowing existing OTLP retries. Deterministic identifiers
include organization, space, Agent, Trace, Span, direction, MIME and byte hash.
Retries deduplicate within that scope. Large integers are parsed with UseNumber.
Only explicit content/message containers are traversed. Native OpenAI
`input_file` / `file.file_data`, Anthropic source blocks and MCP `resource.blob`
use the same projection; file names and resource URIs never trigger reads. URLs are never fetched.
Invalid or oversized explicit Base64 is projected to a small error descriptor;
its exact original field remains available in raw reads.

Supported validated signatures include PNG, JPEG, GIF, WebP, AVIF, BMP, inert
SVG, MP3, WAV, Ogg, FLAC, MP4/M4A, WebM, QuickTime, PDF and UTF-8 plain text,
Markdown and JSON. Browser codec support still determines playback. Active SVG,
HTML documents, untyped Base64 and private third-party file IDs are not loaded.

## API contract

- `GET /api/v1/ai-vision/media/limits`: single-file ceiling.
- `POST /api/v1/ai-vision/media?space_id=...&agent_uuid=...`: multipart `file`,
  `trace_id`, `span_id`, `direction`. Admin access is required.
- `GET /api/v1/ai-vision/media/{id}` and `/{id}/content`: scoped metadata/bytes.
  An active, authorized span must currently reference the same ID and MIME in
  the same direction. An uploaded orphan alone grants no read access.
- `DELETE /api/v1/ai-vision/media/{id}`: Admin cleanup of an unassociated upload;
  cannot delete attachments of an existing span.
- Trace detail and span batch return restored originals by default.
  `media_view=1` returns compact display references. Shadowed payload copies are
  removed during source restoration; exact attribute string values are retained.

Limits: 16 MiB decoded bytes per file, 100 MiB active binary bytes per
organization/space, 30 days binary retention, 64 MiB OTLP request body. A space
quota lock serializes concurrent admission. Retries and expired-record
reactivation obey quota. Hourly cleanup removes bounded batches of expired
binary rows. Exact originals survive while their owning Trace exists; orphaned
original rows are removed after 30 days.

## Historical data and verification

This applies to newly ingested traces. Historical inline fields need reingestion
through this same pipeline to acquire references. Legacy Django UUID attachment references are not served; the abandoned
control-plane migrations are excluded from the prerelease change. New reads
never access those tables, and no historical migration is provided. Missing/filtered source bytes and local paths cannot be recovered.
No automatic remote URL download, public-share media, transcoding or OCR is
provided.

`trace_media_integration_test.go` uses `SIGNOZ_TEST_OCEANBASE_DSN` and unique
temporary tables. It verifies a real 16 MiB write/read, exact originals,
rollback, deduplication and removal of Base64 from both display copies.
`trace_media_test.go` and `trace_media_store_test.go` cover typed parsing,
limits, SVG safety, order/numbers, storage failures and scope validation.
The module tests verify source restoration scope and stale-reference denial.
