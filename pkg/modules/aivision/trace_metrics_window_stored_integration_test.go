package aivision

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/stretchr/testify/require"
)

// Exercise the real generated-column expressions and old-writer updates. The
// wider recovery integration test covers source/maintenance/snapshot fencing.
func TestOceanBaseMetricsWindowStoredFactsPreserveCanonicalSemantics(t *testing.T) {
	dsn := os.Getenv("SIGNOZ_TEST_OCEANBASE_DSN")
	if dsn == "" {
		t.Skip("set isolated SQL DSN for stored metrics semantics")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	ctx := context.Background()
	table := fmt.Sprintf("`metrics_facts_%x`", time.Now().UnixNano())
	columns := [][2]string{{ingest.TraceProjectionMetadataColumn, ingest.TraceProjectionMetadataExpression}, {ingest.TraceProjectionMetricsColumn, ingest.TraceProjectionMetricsExpression}, {ingest.TraceProjectionRowKeysColumn, ingest.TraceProjectionRowKeysExpression}, {ingest.TraceProjectionOperationPreviewsColumn, ingest.TraceProjectionOperationPreviewsExpression}}
	ddl := "CREATE TABLE " + table + " (id INT PRIMARY KEY,row_json LONGTEXT NOT NULL"
	for _, column := range columns {
		ddl += "," + column[0] + " LONGTEXT GENERATED ALWAYS AS (" + column[1] + ") STORED"
	}
	_, err = db.ExecContext(ctx, ddl+")")
	require.NoError(t, err)
	defer func() { _, err := db.Exec("DROP TABLE IF EXISTS " + table); require.NoError(t, err) }()
	cases := []struct {
		name, document string
		metricsPresent bool
		metrics        any
	}{
		{"missing", `{"trace_id":"abc"}`, false, nil},
		{"explicit_null", `{"trace_id":"abc","userId":null,"_metrics":null,"_operation_previews":null}`, true, nil},
		{"malformed_scalar", `{"trace_id":"abc","_metrics":"invalid","_operation_previews":7}`, true, "invalid"},
		{"complete", `{"trace_id":"abc","userId":null,"duration_ms":1.25,"output_tokens":1.0,"total_tokens":1e3,"input_tokens":9007199254740993,"attributes.gen_ai.usage.input_tokens":9007199254740993,"attributes":{"gen_ai":{"usage":{"output_tokens":42}}},"_metrics":{"observations":[{"sessionIds":["s1","s2"],"count":9007199254740993,"duration":1e3}]},"_operation_previews":{"op":{"input":"preview","input_truncated":true}},"attributes.gen_ai.input.messages":"` + strings.Repeat("x", 1<<20) + `","arbitrary.payload":"private"}`, true, nil},
		{"duplicate_keys", `{"trace_id":"abc","name":"first","name":"last","_metrics":{"observations":[{"count":1,"count":2}]}}`, true, nil},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Only row_json is written, just as an older publisher would do.
			_, err = db.ExecContext(ctx, "INSERT INTO "+table+" (id,row_json) VALUES (1,?) ON DUPLICATE KEY UPDATE row_json=VALUES(row_json)", tc.document)
			require.NoError(t, err)
			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			records, err := scanReadModelRows(ctx, tx, "SELECT metadata_json_v1 AS row_json,row_keys_json_v1 AS row_keys,metrics_json_v1 AS metrics,operation_previews_json_v1 AS operation_previews FROM "+table+" WHERE id=1")
			require.NoError(t, err)
			require.Len(t, records, 1)
			metadata, err := tracePageJSON(records[0]["row_json"])
			require.NoError(t, err)
			row, err := traceMetricsWindowStoredRow(metadata, records[0])
			require.NoError(t, err)
			require.Equal(t, "abc", row["trace_id"])
			value, present := row["_metrics"]
			require.Equal(t, tc.metricsPresent, present)
			if i < 3 {
				require.Equal(t, tc.metrics, value)
			}
			require.NotContains(t, row, "environment")
			require.NotContains(t, row, "attributes.gen_ai.input.messages")
			require.NotContains(t, row, "arbitrary.payload")
			if i == 0 {
				require.NotContains(t, row, "userId")
				require.NotContains(t, row, "_operation_previews")
			}
			if i == 1 {
				require.Contains(t, row, "userId")
				require.Nil(t, row["userId"])
				require.Contains(t, row, "_operation_previews")
				require.Nil(t, row["_operation_previews"])
			}
			if i == 4 {
				require.Equal(t, "last", row["name"])
				observation := value.(map[string]any)["observations"].([]any)[0].(map[string]any)
				require.Equal(t, json.Number("2"), observation["count"])
			}
			if i == 3 {
				require.Equal(t, json.Number("9007199254740993"), row["input_tokens"])
				require.Equal(t, json.Number("9007199254740993"), row["attributes.gen_ai.usage.input_tokens"])
				require.NotContains(t, row, "attributes.gen_ai.usage.output_tokens", "nested object is not a literal dotted key")
				require.Equal(t, json.Number("1.25"), row["duration_ms"])
				for _, field := range []string{"output_tokens", "total_tokens"} {
					require.True(t, strings.ContainsAny(row[field].(json.Number).String(), ".eE"), "invalid float token must remain a float for canonical validation: %s", field)
				}
				observation := value.(map[string]any)["observations"].([]any)[0].(map[string]any)
				require.Equal(t, []any{"s1", "s2"}, observation["sessionIds"])
				require.Equal(t, json.Number("9007199254740993"), observation["count"])
				duration, err := observation["duration"].(json.Number).Float64()
				require.NoError(t, err)
				require.Equal(t, float64(1000), duration)
				require.Equal(t, map[string]any{"input": "preview", "input_truncated": true}, row["_operation_previews"].(map[string]any)["op"])
			}
			compact, err := json.Marshal(row)
			require.NoError(t, err)
			require.Less(t, len(compact), 4096)
			require.NoError(t, tx.Commit())
		})
	}
}
