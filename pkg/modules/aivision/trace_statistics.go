package aivision

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"

	"github.com/SigNoz/signoz/pkg/ingest"
)

const traceStatisticsVersion = "native-trace-statistics-v1"
const traceStatisticsAggregateContract = "native-trace-metrics-aggregate-v1"

type statisticsCall struct {
	ID         string          `json:"id"`
	Kind       string          `json:"type"`
	StartNS    string          `json:"start_ns"`
	Model      any             `json:"model"`
	Provider   any             `json:"provider"`
	Ambiguous  bool            `json:"ambiguous"`
	Input      json.RawMessage `json:"input"`
	Output     json.RawMessage `json:"output"`
	Total      json.RawMessage `json:"total"`
	CacheRead  json.RawMessage `json:"cacheReadInput"`
	CacheWrite json.RawMessage `json:"cacheWriteInput"`
	Reasoning  json.RawMessage `json:"reasoning"`
	Cost       struct {
		Total    any `json:"total"`
		Input    any `json:"input"`
		Output   any `json:"output"`
		Currency any `json:"currency"`
	} `json:"cost"`
}
type statisticsFacts struct {
	Version  string `json:"version"`
	Receipt  string `json:"receipt"`
	Declined string `json:"declined,omitempty"`
	Trace    struct {
		SpanCount       int64    `json:"span_count"`
		GenerationCount int64    `json:"generation_count"`
		ToolCount       int64    `json:"tool_count"`
		Latency         *float64 `json:"latency_ms"`
		ErrorCount      int      `json:"error_count"`
	} `json:"trace"`
	Calls []statisticsCall `json:"calls"`
}

var statisticsInteger = regexp.MustCompile(`^(0|[1-9][0-9]{0,64})$`)
var statisticsCost = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]+)?$`)

func statisticsToken(value json.RawMessage) (any, bool) {
	if string(value) == "null" {
		return nil, true
	}
	// The complete window permits at most 500000 calls. Leaving six digits
	// below DECIMAL(65,0) prevents an otherwise valid SQL SUM from overflowing.
	if len(value) > 59 || !statisticsInteger.Match(value) {
		return nil, false
	}
	return string(value), true
}
func statisticsCostValue(value any) (any, bool) {
	if value == nil {
		return nil, true
	}
	text, ok := value.(string)
	if !ok || !statisticsCost.MatchString(text) {
		return nil, false
	}
	parts := strings.SplitN(text, ".", 2)
	if len(parts[0]) > 10 {
		return nil, false
	}
	if len(parts) > 1 && len(strings.TrimRight(parts[1], "0")) > 18 {
		return nil, false
	}
	parsed, ok := new(big.Rat).SetString(text)
	if !ok || parsed.Sign() < 0 || parsed.Cmp(new(big.Rat).SetInt64(10000000000)) >= 0 {
		return nil, false
	}
	return text, true
}

func parseTraceStatistics(p traceProjection) (*statisticsFacts, error) {
	var row struct {
		Statistics json.RawMessage `json:"_statistics"`
	}
	if e := json.Unmarshal(p.Row, &row); e != nil {
		return nil, e
	}
	if len(row.Statistics) == 0 || string(row.Statistics) == "null" {
		return nil, nil
	}
	var facts statisticsFacts
	decoder := json.NewDecoder(strings.NewReader(string(row.Statistics)))
	decoder.UseNumber()
	if e := decoder.Decode(&facts); e != nil {
		return nil, fmt.Errorf("invalid optional statistics: %w", e)
	}
	if facts.Version != traceStatisticsVersion {
		return nil, nil
	}
	if !projectionContextPattern.MatchString(facts.Receipt) {
		return nil, fmt.Errorf("invalid statistics receipt")
	}
	trace := facts.Trace
	if trace.SpanCount != p.NodeCount || trace.GenerationCount < 0 || trace.ToolCount < 0 || trace.GenerationCount+trace.ToolCount > trace.SpanCount || trace.ErrorCount < 0 || trace.ErrorCount > 1 || trace.ErrorCount != boolInt(p.Status == 2) {
		return nil, fmt.Errorf("statistics summary differs from canonical projection")
	}
	if (trace.Latency == nil) != (p.Duration == nil) || trace.Latency != nil && (*trace.Latency != *p.Duration || math.IsInf(*trace.Latency, 0) || math.IsNaN(*trace.Latency)) {
		return nil, fmt.Errorf("statistics latency differs from canonical projection")
	}
	if facts.Declined != "" {
		if facts.Declined != "document_outside_typed_bounds" || len(facts.Calls) != 0 {
			return nil, fmt.Errorf("invalid declined statistics envelope")
		}
		return nil, nil
	}
	if len(facts.Calls) > int(trace.SpanCount) {
		return nil, fmt.Errorf("statistics operation count exceeds spans")
	}
	seen := map[string]bool{}
	var generations, tools int64
	for _, call := range facts.Calls {
		if call.ID == "" || len(call.ID) > 255 || seen[call.ID] {
			return nil, fmt.Errorf("invalid statistics operation identity")
		}
		seen[call.ID] = true
		if call.Kind != "GENERATION" && call.Kind != "EMBEDDING" && call.Kind != "TOOL" {
			return nil, fmt.Errorf("invalid statistics operation type")
		}
		if call.Kind == "GENERATION" {
			generations++
		}
		if call.Kind == "TOOL" {
			tools++
			if call.Ambiguous {
				return nil, fmt.Errorf("tool cannot carry billing ambiguity")
			}
		}
		if _, e := strconv.ParseUint(call.StartNS, 10, 63); e != nil {
			return nil, fmt.Errorf("invalid statistics operation time")
		}
		for _, dimension := range []struct {
			value any
			bound int
		}{{call.Model, 255}, {call.Provider, 255}, {call.Cost.Currency, 16}} {
			if dimension.value != nil {
				text, ok := dimension.value.(string)
				if !ok || len(text) > dimension.bound {
					return nil, nil
				}
			}
		}
		for _, value := range []json.RawMessage{call.Input, call.Output, call.Total, call.CacheRead, call.CacheWrite, call.Reasoning} {
			if _, ok := statisticsToken(value); !ok {
				return nil, nil
			}
		}
		for _, value := range []any{call.Cost.Total, call.Cost.Input, call.Cost.Output} {
			if _, ok := statisticsCostValue(value); !ok {
				return nil, nil
			}
		}
	}
	if generations != trace.GenerationCount || tools != trace.ToolCount {
		return nil, fmt.Errorf("statistics operation count differs from canonical projection")
	}
	return &facts, nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (s *oceanBaseStore) publishStatistics(ctx context.Context, tx *sql.Tx, org string, p traceProjection, capability ...bool) error {
	facts, e := parseTraceStatistics(p)
	if e != nil {
		return e
	}
	eligible := facts != nil
	if facts == nil {
		var row struct {
			Statistics *statisticsFacts `json:"_statistics"`
		}
		decoder := json.NewDecoder(strings.NewReader(string(p.Row)))
		decoder.UseNumber()
		if e = decoder.Decode(&row); e != nil {
			return e
		}
		if row.Statistics == nil || row.Statistics.Version != traceStatisticsVersion {
			return nil
		}
		facts = row.Statistics
	}
	ready := false
	if len(capability) > 0 {
		ready = capability[0]
	} else {
		ready, e = ingest.TraceStatisticsSchemaReady(ctx, tx, s.cfg.TablePrefix)
		if e != nil {
			return e
		}
	}
	if !ready {
		return nil
	}
	// Source CAS and the maintenance fence are already held by the caller. A
	// reprojected Trace replaces its entire contribution, including removed calls.
	for _, suffix := range []string{"call_statistics", "trace_statistics"} {
		if _, e = tx.ExecContext(ctx, "DELETE FROM "+s.readModelTable(suffix)+" WHERE org_id=? AND space_id=? AND trace_id=?", org, p.SpaceID, p.TraceID); e != nil {
			return e
		}
	}
	trace := facts.Trace
	if _, e = tx.ExecContext(ctx, "INSERT INTO "+s.readModelTable("trace_statistics")+" (org_id,space_id,trace_id,source_revision,projection_context,receipt,eligible,span_count,generation_count,tool_count,latency_ms,error_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", org, p.SpaceID, p.TraceID, p.SourceRevision, p.ProjectionContext, facts.Receipt, eligible, trace.SpanCount, trace.GenerationCount, trace.ToolCount, trace.Latency, trace.ErrorCount); e != nil {
		return e
	}
	if !eligible {
		return nil
	}
	for _, call := range facts.Calls {
		values := []any{org, p.SpaceID, p.TraceID, call.ID, call.Kind, call.StartNS, call.Model, call.Provider, call.Ambiguous}
		for _, value := range []json.RawMessage{call.Input, call.Output, call.Total, call.CacheRead, call.CacheWrite, call.Reasoning} {
			v, _ := statisticsToken(value)
			values = append(values, v)
		}
		for _, value := range []any{call.Cost.Total, call.Cost.Input, call.Cost.Output} {
			v, _ := statisticsCostValue(value)
			values = append(values, v)
		}
		values = append(values, call.Cost.Currency)
		query := "INSERT INTO " + s.readModelTable("call_statistics") + " (org_id,space_id,trace_id,observation_id,kind,start_ns,model,provider,ambiguous,input_tokens,output_tokens,total_tokens,cache_read_input_tokens,cache_write_input_tokens,reasoning_tokens,cost_total,cost_input,cost_output,cost_currency) VALUES (" + strings.TrimSuffix(strings.Repeat("?,", len(values)), ",") + ")"
		if _, e = tx.ExecContext(ctx, query, values...); e != nil {
			return e
		}
	}
	return nil
}

func (s *oceanBaseStore) statisticsRelation(org string, p nativeListParams) (string, []any) {
	relation, args := s.windowCleanRelation(org, p)
	join := " JOIN " + s.readModelTable("trace_statistics") + " t ON t.org_id=p.org_id AND t.space_id=p.space_id AND t.trace_id=p.trace_id AND BINARY t.org_id=BINARY p.org_id AND BINARY t.space_id=BINARY p.space_id AND BINARY t.trace_id=BINARY p.trace_id AND t.source_revision=p.source_revision AND BINARY t.projection_context=BINARY p.projection_context AND BINARY t.receipt=BINARY p.statistics_receipt_v1 AND t.eligible=TRUE AND t.span_count=r.span_count"
	return strings.Replace(relation, " WHERE ", join+" WHERE ", 1), args
}
