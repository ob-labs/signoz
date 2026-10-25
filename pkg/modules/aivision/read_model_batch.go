package aivision

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

const projectionBatchLimit = 100
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
	// Each item retains its own source-revision CAS. An interrupted batch may
	// have committed earlier items; retrying them is safe and rechecks revision.
	data := make([]any, 0, len(items))
	for _, p := range items {
		result, err := s.publishTraceProjection(ctx, org, p)
		if err != nil {
			return nil, err
		}
		receipt, ok := result.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid publication receipt")
		}
		receipt["trace_id"] = p.TraceID
		data = append(data, receipt)
	}
	return map[string]any{"supported": true, "contract": "native-trace-publish-batch-v1", "data": data}, nil
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
