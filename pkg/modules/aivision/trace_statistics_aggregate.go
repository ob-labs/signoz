package aivision

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/SigNoz/signoz/pkg/ingest"
)

func (h *handler) statisticsParams(r *http.Request) (nativeListParams, string, string, error) {
	q := r.URL.Query()
	allowed := map[string]bool{"from": true, "to": true, "space_id": true, "user": true, "agent_product": true, "agent_uuid": true, "projection_context": true, "bucket": true, "group_by": true}
	for key, values := range q {
		if !allowed[key] || len(values) != 1 {
			return nativeListParams{}, "", "", fmt.Errorf("unsupported or repeated statistics parameter: %s", key)
		}
	}
	bucket, group := q.Get("bucket"), q.Get("group_by")
	if bucket != "minute" && bucket != "hour" && bucket != "day" {
		return nativeListParams{}, "", "", fmt.Errorf("invalid statistics bucket")
	}
	if group != "model" && group != "provider" {
		return nativeListParams{}, "", "", fmt.Errorf("invalid statistics grouping")
	}
	copyRequest := r.Clone(r.Context())
	copyURL := *r.URL
	copyRequest.URL = &copyURL
	q.Del("bucket")
	q.Del("group_by")
	q.Set("raw", "false")
	copyRequest.URL.RawQuery = q.Encode()
	p, _, e := h.traceWindowParams(copyRequest)
	return p, bucket, group, e
}

func normalizeStatisticsRow(row map[string]any) error {
	for key, value := range row {
		switch {
		case strings.HasSuffix(key, "Known") || strings.HasSuffix(key, "Count") || key == "costCurrencyInvalid" || key == "count":
			n, e := asInt64(value)
			if e != nil || n < 0 {
				return fmt.Errorf("invalid statistics count %s", key)
			}
			row[key] = n
		case strings.HasSuffix(key, "Sum") && key != "latencySum":
			if value == nil {
				continue
			}
			text := fmt.Sprint(value)
			if strings.HasPrefix(key, "cost") {
				amount, ok := new(big.Rat).SetString(text)
				if !ok || amount.Sign() < 0 {
					return fmt.Errorf("invalid statistics Decimal")
				}
				if amount.Cmp(new(big.Rat).SetInt64(10000000000)) >= 0 {
					return tracePageUnsupported{"statistics_decimal_aggregate_bounds"}
				}
			} else if !statisticsInteger.MatchString(text) {
				return tracePageUnsupported{"statistics_integer_aggregate_bounds"}
			}
			row[key] = text
		case key == "latencySum" || key == "latencyP95":
			if value == nil {
				continue
			}
			number, e := strconv.ParseFloat(fmt.Sprint(value), 64)
			if e != nil || math.IsInf(number, 0) || math.IsNaN(number) || number < 0 {
				return fmt.Errorf("invalid statistics latency")
			}
			row[key] = number
		case key == "start_ns":
			if value != nil {
				row[key] = fmt.Sprint(value)
			}
		}
	}
	return nil
}

func (s *oceanBaseStore) traceMetricsAggregate(ctx context.Context, org string, p nativeListParams, bucket, group string) (any, error) {
	if (bucket != "minute" && bucket != "hour" && bucket != "day") || (group != "model" && group != "provider") {
		return nil, fmt.Errorf("invalid statistics bucket or grouping")
	}
	selected, calls, values, e := s.statisticsSelectedRelations(org, p)
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
	var version string
	if e = tx.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); e != nil {
		return nil, e
	}
	// The SQL grouping-sets shape is an optional OceanBase 4.4+ capability.
	// A retained typed schema on an older engine must not bypass this guard.
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
	gateRows, e := scanReadModelRows(ctx, tx, gate, gateArgs...)
	if e != nil {
		return nil, e
	}
	if len(gateRows) != 1 {
		return nil, fmt.Errorf("incomplete statistics gate/summary")
	}
	summary := gateRows[0]
	if e = normalizeStatisticsRow(summary); e != nil {
		return nil, e
	}
	traceCount, e := asInt64(summary["candidateTraceCount"])
	if e != nil {
		return nil, e
	}
	spanCount, e := asInt64(summary["candidateSpanCount"])
	if e != nil {
		return nil, e
	}
	incomplete, e := asInt64(summary["incompleteCount"])
	if e != nil {
		return nil, e
	}
	if traceCount > traceWindowTraceLimit || spanCount > traceWindowSpanLimit {
		return readModelUnsupported("statistics_window_bounds"), nil
	}
	if incomplete != 0 {
		return readModelUnsupported("statistics_window_incomplete"), nil
	}
	for _, key := range []string{"candidateTraceCount", "candidateSpanCount", "incompleteCount"} {
		delete(summary, key)
	}
	latencyCount, _ := asInt64(summary["latencyCount"])
	summary["latencyP95"] = nil
	latencies := []float64{}
	if latencyCount > 0 {
		rows, e := scanReadModelRows(ctx, tx, "SELECT t.latency_ms"+selected+" AND t.latency_ms IS NOT NULL ORDER BY t.latency_ms LIMIT 50001", values...)
		if e != nil {
			return nil, e
		}
		if int64(len(rows)) != latencyCount || len(rows) > 50000 {
			return nil, fmt.Errorf("incomplete bounded statistics latency vector")
		}
		for _, row := range rows {
			number, e := strconv.ParseFloat(fmt.Sprint(row["latency_ms"]), 64)
			if e != nil || math.IsInf(number, 0) || math.IsNaN(number) || number < 0 {
				return nil, fmt.Errorf("invalid statistics latency vector")
			}
			latencies = append(latencies, number)
		}
		rank := (latencyCount*95+99)/100 - 1
		summary["latencyP95"] = latencies[rank]
	}
	// Join contributions only after the complete current source relation has
	// passed its gate. No row_json, content or attributes are selected or parsed.
	width := map[string]int64{"minute": 60000000000, "hour": 3600000000000, "day": 86400000000000}[bucket]
	result := map[string]any{"supported": true, "complete": true, "contract": traceStatisticsAggregateContract, "summary": summary, "latencies": latencies,
		"meta": map[string]any{"complete": true, "representation": "statistics-v1", "space_id": p.SpaceID, "user": p.User, "agent_product": p.AgentProduct, "agent_uuid": p.AgentUUID, "projection_context": p.ProjectionContext, "from": p.From, "to": p.To, "bucket": bucket, "group_by": group, "maintenance_generation": generation}}
	partitions := newStatisticsPartitions()
	if e = scanReadModelRowsEach(ctx, tx, statisticsPartitionsSQL(calls, group, width), values, partitions.consume); e != nil {
		if unsupported, ok := e.(tracePageUnsupported); ok {
			return readModelUnsupported(unsupported.reason), nil
		}
		return nil, e
	}
	groups, e := partitions.finish()
	if e != nil {
		return nil, e
	}
	for key, value := range groups {
		result[key] = value
	}
	if e = tx.Commit(); e != nil {
		return nil, e
	}
	return result, nil
}
