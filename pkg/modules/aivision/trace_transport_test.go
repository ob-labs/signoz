package aivision

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCompactSpanPreservesPayloadFallbackAndAttributePrecedence(t *testing.T) {
	resource := map[string]any{"aivision.space.id": "authorized"}
	row := map[string]any{"name": "normalized", "attributes": map[string]any{"collision": "normalized", "nullable": nil}, "resource": resource, "resource_attributes": resource,
		"payload": map[string]any{"name": "duplicate", "attributes": map[string]any{"fallback": json.Number("9007199254740993"), "collision": "payload", "nullable": 7}, "sessionId": "payload-only", "resource.attributes.aivision.space.id": "payload-only-provenance", "extra": map[string]any{"nested": true}}}
	require.NoError(t, compactNativeSpan(row))
	attrs := row["attributes"].(map[string]any)
	require.Equal(t, "normalized", attrs["collision"])
	require.Nil(t, attrs["nullable"])
	require.Equal(t, json.Number("9007199254740993"), attrs["fallback"])
	payload := row["payload"].(map[string]any)
	require.NotContains(t, payload, "name")
	require.NotContains(t, payload, "attributes")
	require.Equal(t, "payload-only", payload["sessionId"])
	require.Equal(t, "payload-only-provenance", payload["resource.attributes.aivision.space.id"])
	require.NotContains(t, row, "resource.attributes.aivision.space.id")
	require.Equal(t, resource, row["resource"])
	require.NotContains(t, row, "resource_attributes")
}

func TestSourceByteEstimateIncludesEveryVariableWidthSelectedField(t *testing.T) {
	estimate := nativeSpanSourceBytesSQL()
	for _, column := range []string{"trace_id", "span_id", "parent_span_id", "span_name", "span_kind", "status_message", "service_name", "user_id", "space_id", "agent_product", "session_id", "resource_attributes", "attributes", "events", "links", "payload"} {
		require.Contains(t, estimate, "OCTET_LENGTH(COALESCE("+column+",''))")
	}
}
