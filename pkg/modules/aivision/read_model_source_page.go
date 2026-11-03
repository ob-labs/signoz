package aivision

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/SigNoz/signoz/pkg/ingest"
)

const traceSourcePageContract = "native-trace-source-pages-v1"
const sourcePageByteLimit = 2 << 20
const sourcePageFragmentByteLimit = 1280 << 10
const sourcePageTraceByteLimit = 128 << 20
const sourcePageSpanLimit = 128

// The cursor is a scope-bound continuation, never an authorization credential.
// Every request rechecks the current maintenance and physical source fences.
type sourcePageCursor struct {
	Version    int    `json:"v"`
	Org        string `json:"org"`
	Space      string `json:"space"`
	Trace      string `json:"trace"`
	Revision   int64  `json:"revision"`
	Generation int64  `json:"generation"`
	Span       string `json:"span"`
	Offset     int64  `json:"offset"`
	Count      int64  `json:"count"`
	Bytes      int64  `json:"bytes"`
	Completed  int64  `json:"completed"`
	ReadBytes  int64  `json:"read_bytes"`
}

type sourcePageFragment struct {
	SpanID     string `json:"span_id"`
	Offset     int64  `json:"offset"`
	TotalBytes int64  `json:"total_bytes"`
	DataBase64 string `json:"data_base64"`
}

func encodeSourcePageCursor(cursor sourcePageCursor) (string, error) {
	data, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeSourcePageCursor(raw, org, space, trace string) (sourcePageCursor, error) {
	var cursor sourcePageCursor
	if len(raw) > 4096 {
		return cursor, fmt.Errorf("invalid source page cursor")
	}
	data, err := base64.RawURLEncoding.Strict().DecodeString(raw)
	if err != nil {
		return cursor, fmt.Errorf("invalid source page cursor")
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var extra any
	if decoder.Decode(&cursor) != nil || decoder.Decode(&extra) != io.EOF || cursor.Version != 1 || cursor.Org != org || cursor.Space != space || cursor.Trace != trace || cursor.Revision < 1 || cursor.Generation < 1 || cursor.Span == "" || len(cursor.Span) > 255 || cursor.Offset < 0 || cursor.Offset >= sourcePageTraceByteLimit || cursor.Count < 1 || cursor.Count > 100000 || cursor.Bytes < 1 || cursor.Bytes > sourcePageTraceByteLimit || cursor.Completed < 0 || cursor.Completed >= cursor.Count || cursor.ReadBytes < 0 || cursor.ReadBytes >= cursor.Bytes || cursor.Offset > cursor.ReadBytes {
		return sourcePageCursor{}, fmt.Errorf("invalid source page cursor")
	}
	return cursor, nil
}

// JSON columns remain original strings. The consumer parses them with the same
// semantics as decodeWindowSpan, preserving payload fallback fields and exact
// integer values. Byte slicing occurs inside SQL; Go never hydrates a giant Span.
func sourcePageSpanJSONSQL() string {
	fields := []struct{ key, expression string }{
		{"trace_id", "trace_id"}, {"span_id", "span_id"}, {"parent_span_id", "parent_span_id"},
		{"name", "span_name"}, {"kind", "span_kind"}, {"status_code", "status_code"}, {"status_message", "status_message"},
		{"start_time_unix_nano", "CAST(start_time_unix_nano AS CHAR)"}, {"end_time_unix_nano", "CAST(end_time_unix_nano AS CHAR)"}, {"duration_nano", "CAST(duration_nano AS CHAR)"},
		{"service_name", "service_name"}, {"user_id", "user_id"}, {"space_id", "space_id"}, {"agent_product", "agent_product"}, {"session_id", "session_id"},
		{"resource_attributes", "resource_attributes"}, {"attributes", "attributes"}, {"events", "events"}, {"links", "links"}, {"payload", "payload"},
	}
	parts := make([]string, 0, len(fields)*2)
	for _, field := range fields {
		parts = append(parts, "'"+field.key+"'", field.expression)
	}
	return "JSON_OBJECT(" + strings.Join(parts, ",") + ")"
}

func (s *oceanBaseStore) traceSourcePage(ctx context.Context, org, space, trace, rawCursor string) (any, error) {
	if space == "" || len(space) > 255 || !readModelTraceID.MatchString(trace) {
		return nil, fmt.Errorf("invalid source page scope")
	}
	cursor := sourcePageCursor{Version: 1, Org: org, Space: space, Trace: trace}
	var err error
	if rawCursor != "" {
		cursor, err = decodeSourcePageCursor(rawCursor, org, space, trace)
		if err != nil {
			return nil, err
		}
	}
	tx, err := s.beginReadModel(ctx, true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	enabled, generation, err := ingest.ReadProjectionMaintenance(ctx, tx, s.cfg.TablePrefix)
	if err != nil {
		return nil, err
	}
	if !enabled {
		return readModelUnsupported("projection_maintenance_unavailable"), nil
	}
	if generation < 1 {
		return nil, fmt.Errorf("invalid source maintenance generation")
	}
	scope := "org_id=? AND BINARY org_id=BINARY ? AND space_id=? AND BINARY space_id=BINARY ? AND trace_id=? AND BINARY trace_id=BINARY ?"
	args := []any{org, org, space, space, trace, trace}
	var revision, min, max int64
	var user, product string
	var valid bool
	err = tx.QueryRowContext(ctx, "SELECT source_revision,min_timestamp,max_timestamp,user_id,agent_product,scope_valid FROM "+s.readModelTable("trace_sources")+" WHERE "+scope, args...).Scan(&revision, &min, &max, &user, &product, &valid)
	if err == sql.ErrNoRows {
		return readModelUnsupported("source_not_enrolled"), nil
	}
	if err != nil {
		return nil, err
	}
	if !valid {
		return readModelUnsupported("source_scope_unprojectable"), nil
	}
	if revision < 1 {
		return nil, fmt.Errorf("invalid source revision")
	}
	if rawCursor != "" {
		if generation != cursor.Generation {
			return readModelUnsupported("maintenance_generation_changed"), nil
		}
		if revision != cursor.Revision {
			return readModelUnsupported("source_revision_changed"), nil
		}
	}
	// Bound raw source widths before JSON_OBJECT is evaluated, including cursor
	// continuations. Small fragments alone do not bound database allocations.
	var rawCount, rawBytes, maxRawBytes int64
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*),COALESCE(SUM("+nativeSpanSourceBytesSQL()+"),0),COALESCE(MAX("+nativeSpanSourceBytesSQL()+"),0) FROM "+s.tracesTable()+" WHERE "+scope, args...).Scan(&rawCount, &rawBytes, &maxRawBytes); err != nil {
		return nil, err
	}
	if rawCount < 1 || rawCount > 100000 || rawBytes > sourcePageTraceByteLimit || maxRawBytes > 32*1024*1024 {
		return readModelUnsupported("source_trace_payload_limit"), nil
	}
	if rawCursor == "" {
		cursor.Generation, cursor.Revision = generation, revision
		// A single exact Trace aggregate determines the reconstruction budget.
		// It returns only numbers, never LONGTEXT values, to the Go process.
		err = tx.QueryRowContext(ctx, "SELECT COUNT(*),COALESCE(SUM(OCTET_LENGTH("+sourcePageSpanJSONSQL()+")),0) FROM "+s.tracesTable()+" WHERE "+scope, args...).Scan(&cursor.Count, &cursor.Bytes)
		if err != nil {
			return nil, err
		}
		if cursor.Count < 1 || cursor.Count > 100000 {
			return readModelUnsupported("source_empty_or_too_large"), nil
		}
		if cursor.Bytes < 1 || cursor.Bytes > sourcePageTraceByteLimit {
			return readModelUnsupported("source_trace_payload_limit"), nil
		}
	}
	metadataArgs := append(append([]any{}, args...), cursor.Span, sourcePageSpanLimit+1)
	metadata, err := scanReadModelRows(ctx, tx, "SELECT span_id,OCTET_LENGTH("+sourcePageSpanJSONSQL()+") AS total_bytes FROM "+s.tracesTable()+" WHERE "+scope+" AND BINARY span_id>=BINARY ? ORDER BY BINARY span_id LIMIT ?", metadataArgs...)
	if err != nil {
		return nil, err
	}
	if len(metadata) < 1 || len(metadata) > sourcePageSpanLimit+1 {
		return readModelUnsupported("source_snapshot_incomplete"), nil
	}
	fragments := []sourcePageFragment{}
	remaining := int64(sourcePageFragmentByteLimit)
	nextSpan, nextOffset := "", int64(0)
	previous := ""
	for i, row := range metadata {
		id, ok := row["span_id"].(string)
		total, e := asInt64(row["total_bytes"])
		if !ok || id == "" || len(id) > 255 || i > 0 && id <= previous || e != nil || total < 1 || total > cursor.Bytes {
			return nil, fmt.Errorf("invalid source fragment metadata")
		}
		previous = id
		offset := int64(0)
		if i == 0 && cursor.Offset > 0 {
			if id != cursor.Span || cursor.Offset >= total {
				return readModelUnsupported("source_snapshot_incomplete"), nil
			}
			offset = cursor.Offset
		}
		if remaining == 0 || len(fragments) == sourcePageSpanLimit {
			nextSpan = id
			break
		}
		size := total - offset
		if size > remaining {
			size = remaining
		}
		fragments = append(fragments, sourcePageFragment{SpanID: id, Offset: offset, TotalBytes: total})
		remaining -= size
		cursor.ReadBytes += size
		if offset+size < total {
			nextSpan, nextOffset = id, offset+size
			break
		}
		cursor.Completed++
	}
	if cursor.Completed > cursor.Count || cursor.ReadBytes > cursor.Bytes {
		return nil, fmt.Errorf("source reconstruction exceeds declared bounds")
	}
	complete := nextSpan == "" && cursor.Completed == cursor.Count && cursor.ReadBytes == cursor.Bytes
	if nextSpan == "" && !complete {
		return readModelUnsupported("source_snapshot_incomplete"), nil
	}
	queries := []string{}
	chunkArgs := []any{}
	for i, fragment := range fragments {
		size := fragment.TotalBytes - fragment.Offset
		if i == len(fragments)-1 && nextSpan == fragment.SpanID && nextOffset > 0 {
			size = nextOffset - fragment.Offset
		}
		queries = append(queries, "SELECT "+strconv.Itoa(i)+" AS ordinal,span_id,SUBSTRING(CAST("+sourcePageSpanJSONSQL()+" AS BINARY),?,?) AS fragment FROM "+s.tracesTable()+" WHERE "+scope+" AND span_id=? AND BINARY span_id=BINARY ?")
		chunkArgs = append(chunkArgs, fragment.Offset+1, size)
		chunkArgs = append(chunkArgs, args...)
		chunkArgs = append(chunkArgs, fragment.SpanID, fragment.SpanID)
	}
	index := 0
	err = scanReadModelRowsEach(ctx, tx, strings.Join(queries, " UNION ALL ")+" ORDER BY ordinal", chunkArgs, func(row map[string]any) error {
		ordinal, e := asInt64(row["ordinal"])
		if e != nil || index >= len(fragments) || ordinal != int64(index) || row["span_id"] != fragments[index].SpanID {
			return fmt.Errorf("invalid source fragment identity")
		}
		fragment, ok := row["fragment"].(string)
		expected := fragments[index].TotalBytes - fragments[index].Offset
		if index == len(fragments)-1 && nextSpan == fragments[index].SpanID && nextOffset > 0 {
			expected = nextOffset - fragments[index].Offset
		}
		if !ok || int64(len(fragment)) != expected {
			return fmt.Errorf("incomplete source fragment")
		}
		fragments[index].DataBase64 = base64.StdEncoding.EncodeToString([]byte(fragment))
		index++
		return nil
	})
	if err != nil {
		return nil, err
	}
	if index != len(fragments) {
		return readModelUnsupported("source_snapshot_incomplete"), nil
	}
	next := ""
	if !complete {
		cursor.Span, cursor.Offset = nextSpan, nextOffset
		next, err = encodeSourcePageCursor(cursor)
		if err != nil {
			return nil, err
		}
	}
	result := map[string]any{"supported": true, "contract": traceSourcePageContract, "trace_id": trace, "source_revision": revision, "maintenance_generation": generation, "source_user_id": user, "source_agent_product": product, "scope_valid": valid, "span_count": cursor.Count, "source_bytes": cursor.Bytes, "min_timestamp": strconv.FormatInt(min, 10), "max_timestamp": strconv.FormatInt(max, 10), "page_byte_limit": sourcePageByteLimit, "trace_byte_limit": sourcePageTraceByteLimit, "complete": complete, "next_cursor": next, "fragments": fragments}
	wire, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	if len(wire) > sourcePageByteLimit {
		return nil, fmt.Errorf("source page exceeds response byte limit")
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}
