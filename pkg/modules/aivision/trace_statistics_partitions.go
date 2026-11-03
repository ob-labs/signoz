package aivision

import (
	"fmt"
	"strings"
)

// These two scalar/typed relations share the exact candidate and eligibility
// predicates used by statistics and latest-operation selection.
func (s *oceanBaseStore) statisticsSelectedRelations(org string, p nativeListParams) (string, string, []any, error) {
	scope, args, e := traceWindowScope(org, p, "")
	if e != nil {
		return "", "", nil, e
	}
	candidate := "SELECT trace_id,COUNT(*) AS span_count FROM " + s.tracesTable() + " WHERE " + scope + " GROUP BY BINARY trace_id,trace_id"
	relation, cleanArgs := s.statisticsRelation(org, p)
	values := append(append([]any{}, args...), cleanArgs...)
	selected := " FROM (" + candidate + ") r JOIN " + relation
	callJoin := " JOIN " + s.readModelTable("call_statistics") + " c ON c.org_id=t.org_id AND c.space_id=t.space_id AND c.trace_id=t.trace_id AND BINARY c.org_id=BINARY t.org_id AND BINARY c.space_id=BINARY t.space_id AND BINARY c.trace_id=BINARY t.trace_id"
	calls := " FROM (" + candidate + ") r JOIN " + strings.Replace(relation, " WHERE ", callJoin+" WHERE ", 1)
	if p.AgentUUID != "" {
		selected += " AND BINARY p.agent_uuid=BINARY ?"
		calls += " AND BINARY p.agent_uuid=BINARY ?"
		values = append(values, p.AgentUUID)
	}
	return selected, calls, values, nil
}

// The raw candidates remain the driving relation. Invalid/unselected Agents
// cannot disappear before the all-candidate completeness count is computed.
func (s *oceanBaseStore) statisticsGateSummarySQL(org string, p nativeListParams) (string, []any, error) {
	scope, rawArgs, e := traceWindowScope(org, p, "")
	if e != nil {
		return "", nil, e
	}
	candidate := "SELECT trace_id,COUNT(*) AS span_count FROM " + s.tracesTable() + " WHERE " + scope + " GROUP BY BINARY trace_id,trace_id"
	relation, cleanArgs := s.statisticsRelation(org, p)
	joined, predicate, ok := strings.Cut(relation, " WHERE ")
	if !ok {
		return "", nil, fmt.Errorf("invalid statistics clean relation")
	}
	// A nested joined-table operand cannot reference its outer raw candidate.
	// Keep the exact count equality, but place it in the LEFT JOIN's ON clause.
	joined = strings.Replace(joined, " AND t.span_count=r.span_count", "", 1)
	predicate += " AND t.span_count=r.span_count"
	selected := "t.trace_id IS NOT NULL"
	args := []any{}
	if p.AgentUUID != "" {
		selected += " AND BINARY p.agent_uuid=BINARY ?"
		args = append(args, p.AgentUUID)
	}
	facts := "SELECT r.span_count AS candidate_span_count,(t.trace_id IS NOT NULL) AS eligible,(" + selected + ") AS selected,t.span_count,t.generation_count,t.tool_count,t.error_count,t.latency_ms FROM (" + candidate + ") r LEFT JOIN (" + joined + ") ON (" + predicate + ")"
	args = append(args, rawArgs...)
	args = append(args, cleanArgs...)
	query := `SELECT COUNT(*) AS candidateTraceCount,COALESCE(SUM(candidate_span_count),0) AS candidateSpanCount,COALESCE(SUM(CASE WHEN eligible THEN 0 ELSE 1 END),0) AS incompleteCount,COALESCE(SUM(selected),0) AS traceCount,COALESCE(SUM(CASE WHEN selected THEN span_count ELSE 0 END),0) AS spanCount,COALESCE(SUM(CASE WHEN selected THEN generation_count ELSE 0 END),0) AS generationCount,COALESCE(SUM(CASE WHEN selected THEN tool_count ELSE 0 END),0) AS toolCount,COALESCE(SUM(CASE WHEN selected THEN error_count ELSE 0 END),0) AS errorCount,COUNT(CASE WHEN selected THEN latency_ms ELSE NULL END) AS latencyCount,SUM(CASE WHEN selected THEN latency_ms ELSE NULL END) AS latencySum FROM (` + facts + ") gate_summary"
	return query, args, nil
}

var statisticsUsageColumns = []struct{ column, name string }{{"input_tokens", "input"}, {"output_tokens", "output"}, {"total_tokens", "total"}, {"cache_read_input_tokens", "cacheReadInput"}, {"cache_write_input_tokens", "cacheWriteInput"}, {"reasoning_tokens", "reasoning"}, {"cost_total", "costTotal"}, {"cost_input", "costInput"}, {"cost_output", "costOutput"}}

func statisticsConditionalUsageSQL() string {
	billable := "c.kind IN ('GENERATION','EMBEDDING')"
	columns := []string{"COALESCE(SUM(CASE WHEN " + billable + " THEN 1 ELSE 0 END),0) AS callCount", "COALESCE(SUM(CASE WHEN " + billable + " AND c.ambiguous THEN 1 ELSE 0 END),0) AS ambiguousCount", "COALESCE(SUM(CASE WHEN " + billable + " AND (c.cost_currency IS NULL OR BINARY c.cost_currency<>BINARY 'USD') THEN 1 ELSE 0 END),0) AS costCurrencyInvalid"}
	for _, field := range statisticsUsageColumns {
		columns = append(columns, "SUM(CASE WHEN "+billable+" THEN c."+field.column+" ELSE NULL END) AS "+field.name+"Sum", "COALESCE(SUM(CASE WHEN "+billable+" AND c."+field.column+" IS NOT NULL THEN 1 ELSE 0 END),0) AS "+field.name+"Known")
	}
	return strings.Join(columns, ",")
}

func statisticsPartitionsSQL(calls, group string, width int64) string {
	columns := []string{"c.kind", "CASE WHEN c.kind IN ('GENERATION','EMBEDDING') THEN BINARY c." + group + " ELSE NULL END AS dimension", fmt.Sprintf("(c.start_ns DIV %d)*%d AS bucket_ns", width, width), "c.ambiguous", "c.cost_currency"}
	for _, field := range statisticsUsageColumns {
		columns = append(columns, "c."+field.column)
	}
	facts := "SELECT " + strings.Join(columns, ",") + calls
	return "SELECT GROUPING(c.kind) AS grouping_kind,GROUPING(c.dimension) AS grouping_dimension,GROUPING(c.bucket_ns) AS grouping_bucket,c.kind,c.dimension,c.bucket_ns,COUNT(*) AS operationCount," + statisticsConditionalUsageSQL() + " FROM (" + facts + ") c GROUP BY GROUPING SETS ((),(c.kind),(c.dimension),(c.bucket_ns),(c.kind,c.bucket_ns))"
}

// seekdb 1.4 does not accept GROUPING SETS. Expand the one authorized narrow
// input across five explicit axes; the axis keeps actual NULL dimensions apart
// from omitted grouping keys without five copies of the authorization joins.
func statisticsSeekdbPartitionsSQL(calls, group string, width int64) string {
	columns := []string{"c.kind", "CASE WHEN c.kind IN ('GENERATION','EMBEDDING') THEN BINARY c." + group + " ELSE NULL END AS dimension", fmt.Sprintf("(c.start_ns DIV %d)*%d AS bucket_ns", width, width), "c.ambiguous", "c.cost_currency"}
	for _, field := range statisticsUsageColumns {
		columns = append(columns, "c."+field.column)
	}
	facts := "SELECT " + strings.Join(columns, ",") + calls
	expanded := []string{"c.kind", "a.axis", "CASE WHEN a.axis IN (1,4) THEN c.kind ELSE NULL END AS grouped_kind", "CASE WHEN a.axis=2 THEN c.dimension ELSE NULL END AS dimension", "CASE WHEN a.axis IN (3,4) THEN c.bucket_ns ELSE NULL END AS bucket_ns", "c.ambiguous", "c.cost_currency"}
	for _, field := range statisticsUsageColumns {
		expanded = append(expanded, "c."+field.column)
	}
	axes := "SELECT 0 AS axis UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4"
	input := "SELECT " + strings.Join(expanded, ",") + " FROM (" + facts + ") c CROSS JOIN (" + axes + ") a"
	return "SELECT CASE WHEN c.axis IN (1,4) THEN 0 ELSE 1 END AS grouping_kind,CASE WHEN c.axis=2 THEN 0 ELSE 1 END AS grouping_dimension,CASE WHEN c.axis IN (3,4) THEN 0 ELSE 1 END AS grouping_bucket,c.grouped_kind AS kind,c.dimension,c.bucket_ns,COUNT(*) AS operationCount," + statisticsConditionalUsageSQL() + " FROM (" + input + ") c GROUP BY c.axis,c.grouped_kind,c.dimension,c.bucket_ns"
}

func emptyStatisticsUsage() map[string]any {
	row := map[string]any{"callCount": int64(0), "ambiguousCount": int64(0), "costCurrencyInvalid": int64(0)}
	for _, field := range statisticsUsageColumns {
		row[field.name+"Known"] = int64(0)
		row[field.name+"Sum"] = nil
	}
	return row
}

type statisticsPartitions struct {
	result    map[string]any
	seenUsage bool
	rows      int
}

func newStatisticsPartitions() *statisticsPartitions {
	result := map[string]any{"usage": emptyStatisticsUsage()}
	for _, key := range []string{"usageByType", "groups", "timeseries", "operations"} {
		result[key] = []map[string]any{}
	}
	return &statisticsPartitions{result: result}
}
func (p *statisticsPartitions) consume(row map[string]any) error {
	p.rows++
	if e := normalizeStatisticsRow(row); e != nil {
		return e
	}
	flags := []int64{}
	for _, key := range []string{"grouping_kind", "grouping_dimension", "grouping_bucket"} {
		v, e := asInt64(row[key])
		if e != nil || v < 0 || v > 1 {
			return fmt.Errorf("invalid statistics grouping discriminator")
		}
		flags = append(flags, v)
	}
	code := flags[0]*4 + flags[1]*2 + flags[2]
	kind, dimension, bucket := row["kind"], row["dimension"], row["bucket_ns"]
	count, e := asInt64(row["callCount"])
	if e != nil || count < 0 {
		return fmt.Errorf("invalid statistics group count")
	}
	operationCount, e := asInt64(row["operationCount"])
	if e != nil || operationCount < 0 {
		return fmt.Errorf("invalid statistics operation count")
	}
	for _, key := range []string{"grouping_kind", "grouping_dimension", "grouping_bucket", "kind", "dimension", "bucket_ns", "operationCount"} {
		delete(row, key)
	}
	key := ""
	switch code {
	case 7:
		if p.seenUsage {
			return fmt.Errorf("duplicate statistics total group")
		}
		p.seenUsage = true
		p.result["usage"] = row
		return nil
	case 3:
		if count == 0 {
			return nil
		}
		key = "usageByType"
		row["type"] = kind
	case 5:
		if count == 0 {
			return nil
		}
		key = "groups"
		row["value"] = dimension
	case 6:
		if count == 0 {
			return nil
		}
		key = "timeseries"
		row["start_ns"] = fmt.Sprint(bucket)
	case 2:
		key = "operations"
		row = map[string]any{"type": kind, "start_ns": fmt.Sprint(bucket), "count": operationCount}
	default:
		return fmt.Errorf("unexpected statistics grouping set")
	}
	values := p.result[key].([]map[string]any)
	if len(values) >= 150000 {
		return tracePageUnsupported{"statistics_group_bounds"}
	}
	p.result[key] = append(values, row)
	return nil
}
func (p *statisticsPartitions) finish() (map[string]any, error) {
	// CE returns no GROUPING SETS rows for an empty input, including (). The
	// explicit empty accumulator preserves zero coverage and unknown totals.
	if p.rows > 0 && !p.seenUsage {
		return nil, fmt.Errorf("missing statistics total group")
	}
	return p.result, nil
}
func partitionStatisticsRows(rows []map[string]any) (map[string]any, error) {
	p := newStatisticsPartitions()
	for _, row := range rows {
		if e := p.consume(row); e != nil {
			return nil, e
		}
	}
	return p.finish()
}
