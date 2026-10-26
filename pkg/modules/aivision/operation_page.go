package aivision

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strconv"

	"github.com/SigNoz/signoz/pkg/ingest"
)

const traceOperationPageContract = "native-trace-operation-page-v1"

func (h *handler) operationPageParams(r *http.Request) (nativeListParams, int, error) {
	q := r.URL.Query()
	allowed := map[string]bool{"from": true, "to": true, "space_id": true, "user": true, "agent_product": true, "agent_uuid": true, "projection_context": true, "limit": true}
	for key, values := range q {
		if !allowed[key] || len(values) != 1 {
			return nativeListParams{}, 0, fmt.Errorf("unsupported or repeated operation parameter: %s", key)
		}
	}
	limit := 200
	if text, present := q["limit"]; present {
		value, e := strconv.Atoi(text[0])
		if e != nil || value < 1 || value > 200 {
			return nativeListParams{}, 0, fmt.Errorf("operation limit must be between 1 and 200")
		}
		limit = value
	}
	copied := r.Clone(r.Context())
	url := *r.URL
	copied.URL = &url
	q.Del("limit")
	q.Set("raw", "false")
	copied.URL.RawQuery = q.Encode()
	p, _, e := h.traceWindowParams(copied)
	return p, limit, e
}

func (s *oceanBaseStore) traceOperationPage(ctx context.Context, org string, p nativeListParams, limit int) (any, error) {
	if limit < 1 || limit > 200 {
		return nil, fmt.Errorf("operation limit must be between 1 and 200")
	}
	_, calls, args, e := s.statisticsSelectedRelations(org, p)
	if e != nil {
		return nil, e
	}
	tx, e := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	enabled, generation, e := ingest.ReadProjectionMaintenance(ctx, tx, s.cfg.TablePrefix)
	if e != nil {
		return nil, e
	}
	if !enabled {
		return readModelUnsupported("projection_maintenance_unavailable"), nil
	}
	if generation < 1 {
		return nil, fmt.Errorf("invalid operation maintenance generation")
	}
	var version string
	if e = tx.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); e != nil {
		return nil, e
	}
	if !ingest.TraceStatisticsVersionSupported(version) {
		return readModelUnsupported("statistics_engine_unavailable"), nil
	}
	ready, e := ingest.TraceStatisticsSchemaReady(ctx, tx, s.cfg.TablePrefix)
	if e != nil {
		return nil, e
	}
	if !ready {
		return readModelUnsupported("statistics_schema_unavailable"), nil
	}
	gate, gateArgs, e := s.statisticsGateSummarySQL(org, p)
	if e != nil {
		return nil, e
	}
	summaries, e := scanReadModelRows(ctx, tx, gate, gateArgs...)
	if e != nil {
		return nil, e
	}
	if len(summaries) != 1 {
		return nil, fmt.Errorf("incomplete operation candidate gate")
	}
	summary := summaries[0]
	if e = normalizeStatisticsRow(summary); e != nil {
		return nil, e
	}
	traces, e := asInt64(summary["candidateTraceCount"])
	if e != nil {
		return nil, e
	}
	spans, e := asInt64(summary["candidateSpanCount"])
	if e != nil {
		return nil, e
	}
	incomplete, e := asInt64(summary["incompleteCount"])
	if e != nil {
		return nil, e
	}
	if traces > traceWindowTraceLimit || spans > traceWindowSpanLimit {
		return readModelUnsupported("statistics_window_bounds"), nil
	}
	if incomplete != 0 {
		return readModelUnsupported("statistics_window_incomplete"), nil
	}
	selected, e := asInt64(summary["traceCount"])
	if e != nil {
		return nil, e
	}
	data := []map[string]any{}
	if selected > 0 {
		query := "SELECT t.trace_id,c.observation_id,c.kind AS type,c.start_ns,s.source_revision,t.span_count" + calls + " ORDER BY c.start_ns DESC,BINARY t.trace_id DESC,BINARY c.observation_id DESC LIMIT ?"
		values := append(append([]any{}, args...), limit+1)
		rows, e := scanReadModelRows(ctx, tx, query, values...)
		if e != nil {
			return nil, e
		}
		if len(rows) > limit+1 {
			return nil, fmt.Errorf("operation selection exceeds limit")
		}
		var previousTime int64
		previousTrace, previousID := "", ""
		for index, row := range rows {
			trace, traceOK := row["trace_id"].(string)
			id, idOK := row["observation_id"].(string)
			kind, kindOK := row["type"].(string)
			at, e := asInt64(row["start_ns"])
			if e != nil || at < p.From*1000000 || at >= p.To*1000000 {
				return nil, fmt.Errorf("operation outside selected time window")
			}
			revision, e := asInt64(row["source_revision"])
			if e != nil || revision < 1 {
				return nil, fmt.Errorf("invalid operation source revision")
			}
			count, e := asInt64(row["span_count"])
			if e != nil || count < 1 || count > traceWindowSpanLimit {
				return nil, fmt.Errorf("invalid operation source count")
			}
			if !traceOK || !readModelTraceID.MatchString(trace) || !idOK || id == "" || len(id) > 255 || !kindOK || (kind != "GENERATION" && kind != "EMBEDDING" && kind != "TOOL") {
				return nil, fmt.Errorf("invalid operation identity or type")
			}
			if index > 0 && (at > previousTime || at == previousTime && (trace > previousTrace || trace == previousTrace && id >= previousID)) {
				return nil, fmt.Errorf("duplicate or out-of-order operation keys")
			}
			previousTime, previousTrace, previousID = at, trace, id
			data = append(data, map[string]any{"trace_id": trace, "observation_id": id, "type": kind, "start_ns": strconv.FormatInt(at, 10), "source_revision": revision, "span_count": count})
		}
	}
	more := len(data) > limit
	if more {
		data = data[:limit]
	}
	if e = tx.Commit(); e != nil {
		return nil, e
	}
	return map[string]any{"supported": true, "complete": true, "contract": traceOperationPageContract, "has_more": more, "data": data, "meta": map[string]any{"complete": true, "representation": "operations-v1", "space_id": p.SpaceID, "user": p.User, "agent_product": p.AgentProduct, "agent_uuid": p.AgentUUID, "projection_context": p.ProjectionContext, "from": p.From, "to": p.To, "limit": limit, "maintenance_generation": generation}}, nil
}
