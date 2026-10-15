package aivision

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	cmock "github.com/SigNoz/clickhouse-go-mock"
	"github.com/SigNoz/signoz/pkg/telemetrystore"
)

type nativeJSONTestStore struct {
	telemetrystore.TelemetryStore
	rows telemetrystore.Rows
}

func (s nativeJSONTestStore) QueryContext(context.Context, string, ...any) (telemetrystore.Rows, error) {
	return s.rows, nil
}

func queryNativeJSON(t *testing.T, columns []string, values []any) map[string]any {
	t.Helper()
	types := make([]cmock.ColumnType, len(columns))
	for i, column := range columns {
		types[i] = cmock.ColumnType{Name: column, Type: "String"}
	}
	rows := telemetrystore.AdaptClickHouseRows(telemetrystore.WrapRows(cmock.NewRows(types, [][]any{values})), nil)
	store := oceanBaseStore{telemetryStore: nativeJSONTestStore{rows: rows}, cfg: config{MaxLimit: 10}}
	result, err := store.queryRows(context.Background(), "SELECT synthetic_json")
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("got %d rows, want 1", len(result))
	}
	return result[0]
}

func nativeJSONWire(t *testing.T, row map[string]any) map[string]json.RawMessage {
	t.Helper()
	w := httptest.NewRecorder()
	writeJSON(w, http.StatusOK, row)
	if w.Code != http.StatusOK {
		t.Fatalf("wire status = %d", w.Code)
	}
	var envelope struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.Data
}

func TestQueryRowsPreservesOTLPJSONNumbersOnNativeWire(t *testing.T) {
	row := queryNativeJSON(t, []string{"payload", "attributes", "resource_attributes", "events", "links", "body_json"}, []any{
		`{"start_time_unix_nano":1791076323540501433,"end_time_unix_nano":1791076323541455154,"attributes":{"count":9223372036854775807,"fraction":0.125,"ok":true,"missing":null}}`,
		`{"large":9223372036854775807,"usage":13}`,
		`{"large":18446744073709551615}`,
		`[{"timestamp_unix_nano":1791076323540501433}]`,
		`[{"attributes":{"large":9223372036854775807}}]`,
		`{"value":9223372036854775807,"fraction":0.125}`,
	})
	wire := nativeJSONWire(t, row)
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(wire["payload"], &payload); err != nil {
		t.Fatal(err)
	}
	assertEqual(t, "payload start ns", string(payload["start_time_unix_nano"]), "1791076323540501433")
	assertEqual(t, "payload end ns", string(payload["end_time_unix_nano"]), "1791076323541455154")
	var attrs map[string]json.RawMessage
	if err := json.Unmarshal(payload["attributes"], &attrs); err != nil {
		t.Fatal(err)
	}
	assertEqual(t, "nested signed integer", string(attrs["count"]), "9223372036854775807")
	assertEqual(t, "fraction", string(attrs["fraction"]), "0.125")
	assertEqual(t, "boolean", string(attrs["ok"]), "true")
	assertEqual(t, "null", string(attrs["missing"]), "null")
	for column, want := range map[string]string{
		"attributes":          `{"large":9223372036854775807,"usage":13}`,
		"resource_attributes": `{"large":18446744073709551615}`,
		"events":              `[{"timestamp_unix_nano":1791076323540501433}]`,
		"links":               `[{"attributes":{"large":9223372036854775807}}]`,
		"body_json":           `{"fraction":0.125,"value":9223372036854775807}`,
	} {
		assertEqual(t, column+" wire", string(wire[column]), want)
	}
}

func TestQueryRowsStillRejectsMalformedOrMultipleJSONDocuments(t *testing.T) {
	row := queryNativeJSON(t, []string{"payload", "attributes", "body_json", "unrelated"}, []any{
		`{"t":1} {"t":2}`, `{broken`, `{"t":1} trailing`, `{"large":9223372036854775807}`,
	})
	assertEqual(t, "multiple documents stay text", row["payload"], `{"t":1} {"t":2}`)
	assertEqual(t, "malformed stays text", row["attributes"], `{broken`)
	assertEqual(t, "trailing garbage stays text", row["body_json"], `{"t":1} trailing`)
	assertEqual(t, "unrelated text stays text", row["unrelated"], `{"large":9223372036854775807}`)
}

func TestQueryRowsPreservesJSONNumberForms(t *testing.T) {
	for _, document := range []string{
		`{"value":1.234567890123456789e+40}`,
		`{"value":1e-400}`,
		`{"value":-9223372036854775808}`,
		`{"value":0.12345678901234567890123456789}`,
	} {
		t.Run(document, func(t *testing.T) {
			row := queryNativeJSON(t, []string{"body_json"}, []any{document + " \n\t"})
			wire := nativeJSONWire(t, row)
			assertEqual(t, "JSON number representation", string(wire["body_json"]), document)
		})
	}
}

func TestQueryRowsPreservesNumericEventTimestampProjection(t *testing.T) {
	for _, test := range []struct{ number, want string }{
		{"1791076323540501433", "2026-10-04T01:12:03.540501433Z"},
		{"1791076323540501433e0", "2026-10-04T01:12:03.540501433Z"},
		{"1791076323540501433.9", "2026-10-04T01:12:03.540501433Z"},
		{"1e3", "1970-01-01T00:00:00.000001Z"},
		{"1.9", "1970-01-01T00:00:00.000000001Z"},
	} {
		t.Run(test.number, func(t *testing.T) {
			row := queryNativeJSON(t, []string{"events"}, []any{`[{"name":"checkpoint","timestamp_unix_nano":` + test.number + `}]`})
			normalizeNativeSpan(row)
			event := row["events"].([]any)[0].(map[string]any)
			assertEqual(t, "numeric event timestamp", event["time_unix_nano"], test.want)
			if _, exists := event["timestamp_unix_nano"]; exists {
				t.Fatal("event timestamp must be folded into its exact native ISO projection")
			}
		})
	}
}
