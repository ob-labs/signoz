package aivision

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func statisticsFixture() traceProjection {
	p := validProjection()
	p.NodeCount = 2
	d := 2.0
	p.Duration = &d
	p.Row = json.RawMessage(`{"trace_id":"0123456789abcdef0123456789abcdef","start_time_unix_nano":"100000000","_statistics":{"version":"native-trace-statistics-v1","receipt":"` + strings.Repeat("a", 64) + `","trace":{"span_count":2,"generation_count":1,"tool_count":1,"latency_ms":2,"error_count":0},"calls":[{"id":"g","type":"GENERATION","start_ns":"100000000","model":"m","provider":null,"ambiguous":false,"input":1,"output":2,"total":3,"cacheReadInput":null,"cacheWriteInput":null,"reasoning":null,"cost":{"total":"0.1","input":null,"output":null,"currency":"USD"}},{"id":"t","type":"TOOL","start_ns":"100000001","model":null,"provider":null,"ambiguous":false,"input":null,"output":null,"total":null,"cacheReadInput":null,"cacheWriteInput":null,"reasoning":null,"cost":{"total":null,"input":null,"output":null,"currency":null}}]}}`)
	return p
}

func TestStatisticsFactsValidateCountsAndExactNumericBounds(t *testing.T) {
	p := statisticsFixture()
	facts, e := parseTraceStatistics(p)
	require.NoError(t, e)
	require.NotNil(t, facts)
	require.Len(t, facts.Calls, 2)
	p.NodeCount = 3
	_, e = parseTraceStatistics(p)
	require.Error(t, e)
	p = statisticsFixture()
	p.Row = json.RawMessage(strings.Replace(string(p.Row), `"0.1"`, `"0.0000000000000000001"`, 1))
	facts, e = parseTraceStatistics(p)
	require.NoError(t, e)
	require.Nil(t, facts, "unrepresentable optional facts must leave canonical publication usable")
}

func TestStatisticsSQLNeverReadsCanonicalDocuments(t *testing.T) {
	store := &oceanBaseStore{cfg: config{TablePrefix: "test"}}
	sql, args := store.statisticsRelation("org", nativeListParams{SpaceID: "space", From: 1, To: 2, ProjectionContext: strings.Repeat("b", 64)})
	require.NotContains(t, sql, "row_json")
	require.Contains(t, sql, "statistics_receipt_v1")
	require.Contains(t, sql, "BINARY t.receipt=BINARY p.statistics_receipt_v1")
	require.NotEmpty(t, args)
}

func TestStatisticsUnrepresentableDimensionsLeaveBasePublishUsable(t *testing.T) {
	for _, field := range []string{"model", "provider"} {
		for _, value := range []string{"123", strings.Repeat("9", 400), "true"} {
			p := statisticsFixture()
			old := `"model":"m"`
			if field == "provider" {
				old = `"provider":null`
			}
			p.Row = json.RawMessage(strings.Replace(string(p.Row), old, `"`+field+`":`+value, 1))
			facts, e := parseTraceStatistics(p)
			require.NoError(t, e)
			require.Nil(t, facts)
		}
	}
}

func TestStatisticsDeclinedDocumentKeepsAnIneligibleReceipt(t *testing.T) {
	p := statisticsFixture()
	var row map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(p.Row, &row))
	var facts map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(row["_statistics"], &facts))
	facts["calls"] = json.RawMessage(`[]`)
	facts["declined"] = json.RawMessage(`"document_outside_typed_bounds"`)
	row["_statistics"], _ = json.Marshal(facts)
	p.Row, _ = json.Marshal(row)
	parsed, e := parseTraceStatistics(p)
	require.NoError(t, e)
	require.Nil(t, parsed)
}
