package aivision

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/SigNoz/signoz/pkg/ingest"
)

const projectionBatchLimit = 100
const projectionPublishChunkLimit = 25
const projectionBatchPayloadLimit = 16 * 1024 * 1024

func (s *oceanBaseStore) traceSourceBatch(ctx context.Context, org, space string, ids []string) (any, error) {
	if space == "" || len(space) > 255 || len(ids) < 1 || len(ids) > projectionBatchLimit {
		return nil, fmt.Errorf("invalid source batch scope or count")
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if !readModelTraceID.MatchString(id) || seen[id] {
			return nil, fmt.Errorf("invalid or duplicate source batch Trace ID")
		}
		seen[id] = true
	}
	return s.snapshotTraceBatch(ctx, org, space, ids)
}

func (s *oceanBaseStore) publishTraceBatch(ctx context.Context, org string, items []traceProjection) (any, error) {
	if len(items) < 1 || len(items) > projectionBatchLimit {
		return nil, fmt.Errorf("invalid publication batch count")
	}
	seen := map[string]bool{}
	for _, p := range items {
		if err := validateTraceProjection(p); err != nil {
			return nil, err
		}
		key := p.SpaceID + "/" + p.TraceID
		if seen[key] {
			return nil, fmt.Errorf("duplicate publication batch Trace")
		}
		seen[key] = true
	}
	// Earlier chunks may remain committed if a later chunk fails. Each chunk
	// reacquires the fence and retains every per-Trace revision/scope CAS.
	data := make([]any, 0, len(items))
	for offset := 0; offset < len(items); offset += projectionPublishChunkLimit {
		end := offset + projectionPublishChunkLimit
		if end > len(items) {
			end = len(items)
		}
		committed, err := s.publishTraceChunk(ctx, org, items[offset:end])
		if err != nil {
			return nil, err
		}
		// No tentative success escapes a failed transaction/commit.
		data = append(data, committed...)
	}
	return map[string]any{"supported": true, "contract": "native-trace-publish-batch-v1", "data": data}, nil
}

func (s *oceanBaseStore) publishTraceChunk(ctx context.Context, org string, items []traceProjection) ([]any, error) {
	if len(items) < 1 || len(items) > projectionPublishChunkLimit {
		return nil, fmt.Errorf("invalid publication chunk count")
	}
	tx, err := s.beginReadModel(ctx, false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	enabled, generation, err := ingest.LockProjectionMaintenance(ctx, tx, s.cfg.TablePrefix)
	if err != nil {
		return nil, err
	}
	// Capability belongs to this transaction, not to a process-wide cache. A
	// batch backfill inspects optional metadata once for at most 25 Traces.
	statisticsCapability := []bool{}
	if enabled {
		for _, item := range items {
			if bytes.Contains(item.Row, []byte(`"_statistics"`)) {
				ready, e := ingest.TraceStatisticsSchemaReady(ctx, tx, s.cfg.TablePrefix)
				if e != nil {
					return nil, e
				}
				statisticsCapability = []bool{ready}
				break
			}
		}
	}
	data := make([]any, 0, len(items))
	for _, p := range items {
		receipt := readModelUnsupported("projection_maintenance_unavailable")
		if enabled {
			receipt, err = s.publishTraceProjectionInTx(ctx, tx, org, p, generation, statisticsCapability...)
			if err != nil {
				return nil, err
			}
		}
		receipt["trace_id"] = p.TraceID
		data = append(data, receipt)
	}
	if enabled {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (h *handler) projectionBatchAction(r *http.Request, ctx context.Context, org, action string) (any, error) {
	if action == "batch-snapshot" {
		q := r.URL.Query()
		for key, values := range q {
			if (key != "space_id" && key != "trace_ids") || len(values) != 1 {
				return nil, fmt.Errorf("invalid batch snapshot parameter: %s", key)
			}
		}
		var ids []string
		if err := json.Unmarshal([]byte(q.Get("trace_ids")), &ids); err != nil {
			return nil, fmt.Errorf("trace_ids must be a JSON string array")
		}
		return h.backend.traceSourceBatch(ctx, org, q.Get("space_id"), ids)
	}
	var body struct {
		Projections []traceProjection `json:"projections"`
	}
	if err := decodeReadModelJSON(r, &body); err != nil {
		return nil, err
	}
	return h.backend.publishTraceBatch(ctx, org, body.Projections)
}
