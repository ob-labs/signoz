package aivision

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/SigNoz/signoz/pkg/ingest"
)

// The selected keys and each preview map are read in one repeatable-read view.
// The map excludes unrelated full I/O and is fetched once per selected Trace.
// At most one source map is resident; only selected complete previews survive.
func (s *oceanBaseStore) operationPagePreviews(ctx context.Context, tx *sql.Tx, org string, p nativeListParams, rows []map[string]any) (map[string]map[string]map[string]any, string, error) {
	result := map[string]map[string]map[string]any{}
	if len(rows) == 0 {
		return result, "", nil
	}
	ids := []string{}
	wanted := map[string]map[string]bool{}
	revisions := map[string]int64{}
	clauses := []string{}
	args := []any{org, p.SpaceID, p.ProjectionContext}
	for _, row := range rows {
		trace, id := row["trace_id"].(string), row["observation_id"].(string)
		revision, err := asInt64(row["source_revision"])
		if err != nil {
			return nil, "", err
		}
		if wanted[trace] == nil {
			ids = append(ids, trace)
			wanted[trace] = map[string]bool{}
			revisions[trace] = revision
			clauses = append(clauses, "(p.trace_id=? AND BINARY p.trace_id=BINARY ? AND p.source_revision=?)")
			args = append(args, trace, trace, revision)
		} else if revisions[trace] != revision {
			return nil, "", fmt.Errorf("inconsistent operation source revision")
		}
		wanted[trace][id] = true
	}
	scope := "p.org_id=? AND p.space_id=? AND BINARY p.projection_context=BINARY ?"
	column := "p." + ingest.TraceProjectionOperationPreviewsColumn
	sizes, err := scanReadModelRows(ctx, tx, "SELECT p.trace_id,p.source_revision,OCTET_LENGTH("+column+") AS preview_bytes FROM "+s.readModelTable("trace_projections")+" p WHERE "+scope+" AND ("+strings.Join(clauses, " OR ")+")", args...)
	if err != nil {
		return nil, "", err
	}
	seen := map[string]bool{}
	for _, row := range sizes {
		trace, ok := row["trace_id"].(string)
		revision, e := asInt64(row["source_revision"])
		if !ok || wanted[trace] == nil || seen[trace] || e != nil || revision != revisions[trace] {
			return nil, "", fmt.Errorf("incomplete operation preview preflight")
		}
		seen[trace] = true
		if row["preview_bytes"] == nil {
			return nil, "selected_operation_preview_unavailable", nil
		}
		bytes, e := asInt64(row["preview_bytes"])
		if e != nil {
			return nil, "", e
		}
		// Accepted API publications are at most 16 MiB. This larger source cap
		// also tolerates serialization growth and safe historical direct writers.
		if bytes < 0 || bytes > sourcePageTraceByteLimit {
			return nil, "operation_page_source_payload_limit", nil
		}
	}
	if len(seen) != len(ids) {
		return nil, "selected_operation_preview_unavailable", nil
	}
	for _, trace := range ids {
		var source string
		err := tx.QueryRowContext(ctx, "SELECT "+column+" FROM "+s.readModelTable("trace_projections")+" p WHERE "+scope+" AND p.trace_id=? AND BINARY p.trace_id=BINARY ? AND p.source_revision=?", org, p.SpaceID, p.ProjectionContext, trace, trace, revisions[trace]).Scan(&source)
		if err != nil {
			return nil, "", err
		}
		if len(source) > sourcePageTraceByteLimit {
			return nil, "operation_page_source_payload_limit", nil
		}
		selected, err := selectedOperationPreviews(source, wanted[trace])
		if err != nil {
			return nil, "selected_operation_preview_unavailable", nil
		}
		result[trace] = selected
	}
	return result, "", nil
}

func selectedOperationPreviews(source string, wanted map[string]bool) (map[string]map[string]any, error) {
	decoder := json.NewDecoder(strings.NewReader(source))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("invalid operation preview map")
	}
	selected := map[string]map[string]any{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		id, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("invalid operation preview identity")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		if wanted[id] {
			if _, duplicate := selected[id]; duplicate {
				return nil, fmt.Errorf("duplicate operation preview")
			}
			preview, err := tracePageJSON([]byte(value))
			if err != nil || validateOperationPreview(preview) != nil {
				return nil, fmt.Errorf("invalid selected operation preview")
			}
			selected[id] = preview
		}
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF || len(selected) != len(wanted) {
		return nil, fmt.Errorf("incomplete operation preview map")
	}
	return selected, nil
}
