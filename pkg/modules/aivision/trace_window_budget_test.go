package aivision

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWindowWireBudgetCoversActualHTTPEncoding(t *testing.T) {
	rows := []map[string]any{{"trace_id": "clean", "usage": map[string]any{"total": json.Number("9007199254740993"), "ratio": 1e-9}, "input": "<script>\u2028\x00中文"}}
	details := []map[string]any{{"traceId": strings.Repeat("\x00<", 16), "spans": []map[string]any{{"span_id": "a", "attributes": map[string]any{"input": strings.Repeat("\\\n\"<>&中文", 300), "large": json.Number("18446744073709551615")}}, {"span_id": "b", "end": 1e30}}}, {"traceId": "second", "spans": []map[string]any{{"span_id": "c"}}}}
	meta := map[string]any{"from": int64(1), "to": int64(9223372036854), "space_id": strings.Repeat("\x00", 255), "user": strings.Repeat("<", 255), "agent_product": strings.Repeat("\n", 255), "agent_uuid": strings.Repeat("\\", 255), "projection_context": strings.Repeat("a", 64), "traceCount": 50000, "spanCount": 500000, "raw": false}
	budget := newTraceWindowWireBudget(traceWindowPayloadLimit)
	for _, row := range rows {
		require.NoError(t, budget.addRow(row))
	}
	for _, detail := range details {
		require.NoError(t, budget.addRawGroup(detail["traceId"].(string)))
		for _, row := range detail["spans"].([]map[string]any) {
			require.NoError(t, budget.addRow(row))
		}
	}
	require.NoError(t, budget.validateEnvelope(meta))
	recorder := httptest.NewRecorder()
	writeJSON(recorder, 200, traceWindowResponse(meta, rows, details))
	require.Equal(t, 200, recorder.Code)
	require.LessOrEqual(t, recorder.Body.Len(), budget.used)
	require.LessOrEqual(t, budget.used, traceWindowPayloadLimit)
}

func TestWindowWireBudgetRejectsEscapedGroupsBeforeAccumulating(t *testing.T) {
	budget := newTraceWindowWireBudget(traceWindowEnvelopeReserve + 1024)
	require.ErrorContains(t, budget.addRawGroup(strings.Repeat("\x00", 200)), "256 MiB response")
	require.Equal(t, traceWindowEnvelopeReserve, budget.used)
}

func TestWindowWireBudgetRejectsOversizedMetadataAndRows(t *testing.T) {
	budget := newTraceWindowWireBudget(traceWindowEnvelopeReserve + 1024)
	require.ErrorContains(t, budget.validateEnvelope(map[string]any{"agent_uuid": strings.Repeat("\x00", traceWindowEnvelopeReserve)}), "metadata")
	require.ErrorContains(t, budget.addRow(map[string]any{"input": strings.Repeat("\x00", 200)}), "256 MiB response")
	require.Equal(t, traceWindowEnvelopeReserve, budget.used)
}
