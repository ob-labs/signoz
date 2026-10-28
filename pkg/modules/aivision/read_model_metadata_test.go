package aivision

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// JSON_OBJECT/JSON_EXTRACT are exercised against a real SQL engine. The 800 KiB
// unknown SDK field must never be selected into a metadata response, while flat
// dotted aliases and exact canonical evidence survive the SQL projection.
func TestCanonicalMetadataProjectionDropsGiantUnknownSDKAttributes(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	row := map[string]any{
		"trace_id": "abc", "name": "root", "userId": nil, "session_id": "session", "environment": "prod", "environmentSource": "root-span-resource-v1",
		"start_time_unix_nano": "100000000", "end_time_unix_nano": nil, "duration_ms": nil, "status_code": 1, "span_count": 3,
		"usage":    map[string]any{"total": json.Number("9007199254740993"), "cost": map[string]any{"total": "1.0001"}},
		"coverage": map[string]any{"knownTokens": json.Number("9007199254740993")}, "costCoverage": map[string]any{"known": 2}, "reportedUsage": map[string]any{"input": 2},
		"rootObservationId": "root", "rootSelectionReason": "root_span", "missingParentIds": []string{}, "_traceTags": []string{"tag"}, "_sceneValues": []string{"scene"},
		"resource.attributes.aivision.agent.id": "agent", "resource.attributes.aivision.space.id": "space", "resource.attributes.ant.agent.product": "product",
		"attributes.gen_ai.request.model": "flat-model", "attributes.gen_ai.provider.name": "provider", "attributes.gen_ai.span.kind": "AGENT", "attributes.gen_ai.operation.name": "invoke_agent",
		"resource.attributes.ant.agent.env": "prod", "resource.attributes.ant.agent.scene": "scene", "attributes.ant.username": "sdk-user", "attributes.gen_ai.tool.call.name": "tool",
		"attributes":                            map[string]any{"gen_ai": map[string]any{"request": map[string]any{"model": "wrong-nested-model"}}},
		"attributes.langfuse.observation.input": strings.Repeat("x", 800<<10), "attributes.arbitrary.large": strings.Repeat("y", 800<<10),
	}
	for _, field := range []string{"attributes.gen_ai.input.messages", "attributes.gen_ai.output.messages", "attributes.gen_ai.tool.call.arguments", "attributes.gen_ai.tool.call.result"} {
		row[field] = strings.Repeat("raw", 800<<10)
	}
	wire, err := json.Marshal(row)
	require.NoError(t, err)
	var selected string
	require.NoError(t, db.QueryRow("SELECT "+sessionSummarySQL()+" FROM (SELECT ? AS row_json) p", string(wire)).Scan(&selected))
	compact, err := tracePageJSON(selected)
	require.NoError(t, err)
	require.Less(t, len(selected), 16<<10)
	for _, field := range []string{"costCoverage", "usage", "coverage", "reportedUsage", "rootObservationId", "rootSelectionReason", "missingParentIds", "_traceTags", "_sceneValues", "resource.attributes.aivision.agent.id", "resource.attributes.aivision.space.id", "attributes.gen_ai.provider.name", "attributes.gen_ai.operation.name", "resource.attributes.ant.agent.env", "resource.attributes.ant.agent.scene", "attributes.ant.username", "attributes.gen_ai.tool.call.name"} {
		require.Contains(t, compact, field)
	}
	require.Equal(t, "flat-model", compact["attributes.gen_ai.request.model"])
	require.Equal(t, json.Number("9007199254740993"), compact["usage"].(map[string]any)["total"])
	require.Equal(t, "1.0001", compact["usage"].(map[string]any)["cost"].(map[string]any)["total"])
	for _, field := range []string{"attributes.gen_ai.input.messages", "attributes.gen_ai.output.messages", "attributes.gen_ai.tool.call.arguments", "attributes.gen_ai.tool.call.result"} {
		require.Contains(t, compact, field)
		require.Nil(t, compact[field])
	}
	for _, field := range []string{"attributes.langfuse.observation.input", "attributes.arbitrary.large", "attributes", "_operation_previews", "_statistics", "_metrics"} {
		require.NotContains(t, compact, field)
	}
}
