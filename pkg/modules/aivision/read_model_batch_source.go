package aivision

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/SigNoz/signoz/pkg/ingest"
)

type batchTraceSource struct {
	revision, min, max int64
	user, product      string
	valid              bool
	count, seen        int64
	reason             string
	spans              []map[string]any
	spanIDs            map[string]bool
	wireBytes          int
}

func (s *oceanBaseStore) snapshotTraceBatch(ctx context.Context, org, space string, ids []string) (any, error) {
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
	scope := "org_id=? AND BINARY org_id=BINARY ? AND space_id=? AND BINARY space_id=BINARY ? AND trace_id IN (" + strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",") + ")"
	args := []any{org, org, space, space}
	requested := map[string]bool{}
	for _, id := range ids {
		args = append(args, id)
		requested[id] = true
	}
	sources := map[string]*batchTraceSource{}
	err = scanReadModelRowsEach(ctx, tx, "SELECT trace_id,source_revision,min_timestamp,max_timestamp,user_id,agent_product,scope_valid FROM "+s.readModelTable("trace_sources")+" WHERE "+scope, args, func(row map[string]any) error {
		id, ok := row["trace_id"].(string)
		if !ok || !requested[id] || sources[id] != nil {
			return fmt.Errorf("invalid batch source identity")
		}
		source := &batchTraceSource{spans: []map[string]any{}, spanIDs: map[string]bool{}}
		var err error
		if source.revision, err = asInt64(row["source_revision"]); err != nil {
			return err
		}
		if source.min, err = asInt64(row["min_timestamp"]); err != nil {
			return err
		}
		if source.max, err = asInt64(row["max_timestamp"]); err != nil {
			return err
		}
		source.user, ok = row["user_id"].(string)
		if !ok {
			return fmt.Errorf("invalid batch source physical user")
		}
		source.product, ok = row["agent_product"].(string)
		if !ok {
			return fmt.Errorf("invalid batch source physical product")
		}
		if value, ok := row["scope_valid"].(bool); ok {
			source.valid = value
		} else {
			value, e := asInt64(row["scope_valid"])
			if e != nil || value < 0 || value > 1 {
				return fmt.Errorf("invalid batch source scope state")
			}
			source.valid = value == 1
		}
		sources[id] = source
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(sources) > 0 {
		err = scanReadModelRowsEach(ctx, tx, "SELECT trace_id,COUNT(*) AS span_count,COALESCE(SUM("+nativeSpanSourceBytesSQL()+"),0) AS source_bytes,COALESCE(MAX("+nativeSpanSourceBytesSQL()+"),0) AS maximum_span_bytes FROM "+s.tracesTable()+" WHERE "+scope+" GROUP BY BINARY trace_id,trace_id", args, func(row map[string]any) error {
			id, ok := row["trace_id"].(string)
			if !ok || !requested[id] {
				return fmt.Errorf("invalid batch source count identity")
			}
			source := sources[id]
			if source == nil {
				return nil
			}
			if source.count != 0 {
				return fmt.Errorf("duplicate batch source count")
			}
			count, e := asInt64(row["span_count"])
			if e != nil || count < 0 {
				return fmt.Errorf("invalid batch source count")
			}
			source.count = count
			bytes, e := asInt64(row["source_bytes"])
			if e != nil || bytes < 0 {
				return fmt.Errorf("invalid batch source byte count")
			}
			maxBytes, e := asInt64(row["maximum_span_bytes"])
			if e != nil || maxBytes < 0 {
				return fmt.Errorf("invalid batch source maximum bytes")
			}
			if count == 0 || count > 100000 {
				source.reason = "source_empty_or_too_large"
			} else if bytes > projectionBatchPayloadLimit || maxBytes > traceSpanSourceByteLimit {
				source.reason = "source_snapshot_payload_limit"
			} else {
				source.wireBytes = int(bytes)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	eligible := []string{}
	sourceBytes := int64(0)
	for _, id := range ids {
		source := sources[id]
		if source == nil {
			continue
		}
		if source.count == 0 && source.reason == "" {
			source.reason = "source_empty_or_too_large"
		}
		if source.reason == "" {
			eligible = append(eligible, id)
			sourceBytes += int64(source.wireBytes)
			source.wireBytes = 0
		}
	}
	if sourceBytes > projectionBatchPayloadLimit {
		return readModelUnsupported("source_batch_payload_limit"), nil
	}
	wireBytes := 0
	if len(eligible) > 0 {
		rawScope := "org_id=? AND BINARY org_id=BINARY ? AND space_id=? AND BINARY space_id=BINARY ? AND trace_id IN (" + strings.TrimSuffix(strings.Repeat("?,", len(eligible)), ",") + ")"
		rawArgs := []any{org, org, space, space}
		for _, id := range eligible {
			rawArgs = append(rawArgs, id)
		}
		err = scanReadModelRowsEach(ctx, tx, "SELECT "+nativeSpanProjection+" FROM "+s.tracesTable()+" WHERE "+rawScope+" ORDER BY BINARY trace_id,timestamp,BINARY span_id", rawArgs, func(row map[string]any) error {
			id, ok := row["trace_id"].(string)
			source := sources[id]
			if !ok || source == nil || source.reason != "" && source.reason != "source_json_invalid" {
				return fmt.Errorf("unexpected raw batch source")
			}
			source.seen++
			if source.seen > source.count {
				return fmt.Errorf("batch source span count grew inside snapshot")
			}
			if source.reason != "" {
				return nil
			}
			spanID, ok := row["span_id"].(string)
			if !ok || spanID == "" || source.spanIDs[spanID] {
				return fmt.Errorf("duplicate or missing batch source Span ID")
			}
			source.spanIDs[spanID] = true
			if err := decodeWindowSpan(row); err != nil {
				source.reason = "source_json_invalid"
				source.spans = nil
				wireBytes -= source.wireBytes
				source.wireBytes = 0
				return nil
			}
			if err := compactNativeSpan(row); err != nil {
				return err
			}
			size, e := encodedSpanBytes(row)
			if e != nil {
				return e
			}
			wireBytes += size
			source.wireBytes += size
			if wireBytes > projectionBatchPayloadLimit-1024*len(ids) {
				return tracePageUnsupported{"source_batch_payload_limit"}
			}
			source.spans = append(source.spans, row)
			return nil
		})
		if unsupported, ok := err.(tracePageUnsupported); ok {
			return readModelUnsupported(unsupported.reason), nil
		}
		if err != nil {
			return nil, err
		}
	}
	data := make([]any, 0, len(ids))
	for _, id := range ids {
		source := sources[id]
		reason := ""
		if source == nil {
			reason = "source_not_enrolled"
		} else if source.reason != "" {
			reason = source.reason
		} else if source.seen != source.count {
			reason = "source_snapshot_incomplete"
		}
		if reason != "" {
			receipt := readModelUnsupported(reason)
			receipt["trace_id"] = id
			data = append(data, receipt)
			continue
		}
		data = append(data, map[string]any{"supported": true, "source_user_id": source.user, "source_agent_product": source.product, "scope_valid": source.valid, "contract": "native-trace-source-v1", "trace_id": id, "source_revision": source.revision, "maintenance_generation": generation, "span_count": source.count, "min_timestamp": strconv.FormatInt(source.min, 10), "max_timestamp": strconv.FormatInt(source.max, 10), "complete": true, "spans": source.spans})
	}
	result := map[string]any{"supported": true, "contract": "native-trace-source-batch-v1", "data": data}
	size, err := encodedSpanBytes(result)
	if err != nil {
		return nil, err
	}
	if size > projectionBatchPayloadLimit {
		return readModelUnsupported("source_batch_payload_limit"), nil
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}
