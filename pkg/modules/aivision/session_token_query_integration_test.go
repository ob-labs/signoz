package aivision

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/SigNoz/signoz/pkg/valuer"
	"github.com/stretchr/testify/require"
)

// This fixture checks the actual group builder against literal numeric results,
// including the existing JSON comparison behavior for malformed billable types.
// It creates and drops only its newly named table, after an explicit test DSN.
func TestOceanBaseSessionTokenQuerySemantics(t *testing.T) {
	dsn := os.Getenv("SIGNOZ_TEST_OCEANBASE_DSN")
	if dsn == "" {
		t.Skip("set isolated test DSN for real SQL validation")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := context.Background()
	table := "`session_token_" + strings.ReplaceAll(valuer.GenerateUUID().StringValue(), "-", "")[:12] + "`"
	_, err = db.ExecContext(ctx, "CREATE TABLE "+table+" (session_id VARCHAR(128),org_id VARCHAR(128),space_id VARCHAR(128),agent_uuid VARCHAR(128),agent_product VARCHAR(128),user_id VARCHAR(128),start_ns BIGINT,end_ns BIGINT,status INT,row_json LONGTEXT NOT NULL,"+ingest.TraceProjectionMetadataColumn+" LONGTEXT GENERATED ALWAYS AS ("+ingest.TraceProjectionMetadataExpression+") STORED)")
	require.NoError(t, err)
	t.Cleanup(func() { _, e := db.Exec("DROP TABLE IF EXISTS " + table); require.NoError(t, e) })
	fixtures := []struct {
		name, document, total string
		billable, known       int
	}{
		{"known_zero", `{"coverage":{"billableCalls":1},"usage":{"total":0}}`, "0", 1, 1},
		{"known_integer", `{"coverage":{"billableCalls":2},"usage":{"total":27}}`, "27", 1, 1},
		{"unknown_null", `{"coverage":{"billableCalls":1},"usage":{"total":null}}`, "", 1, 0},
		{"unknown_missing_total", `{"coverage":{"billableCalls":1},"usage":{}}`, "", 1, 0},
		{"missing_usage", `{"coverage":{"billableCalls":1}}`, "", 1, 0},
		{"missing_coverage", `{"usage":{"total":7}}`, "", 0, 0},
		{"null_billable", `{"coverage":{"billableCalls":null},"usage":{"total":7}}`, "", 0, 0},
		{"nonbillable_unknown", `{"coverage":{"billableCalls":0},"usage":{"total":null}}`, "0", 0, 1},
		{"nonbillable_known", `{"coverage":{"billableCalls":0},"usage":{"total":13}}`, "0", 0, 1},
		{"numeric_string_total", `{"coverage":{"billableCalls":1},"usage":{"total":"7"}}`, "", 1, 0},
		{"boolean_total", `{"coverage":{"billableCalls":1},"usage":{"total":true}}`, "", 1, 0},
		{"object_total", `{"coverage":{"billableCalls":1},"usage":{"total":{"a":1}}}`, "", 1, 0},
		{"array_total", `{"coverage":{"billableCalls":1},"usage":{"total":[1,2]}}`, "", 1, 0},
		{"above_js_safe_integer", `{"coverage":{"billableCalls":1},"usage":{"total":9007199254740993}}`, "9007199254740993", 1, 1},
		{"max_uint64", `{"coverage":{"billableCalls":1},"usage":{"total":18446744073709551615}}`, "18446744073709551615", 1, 1},
		{"negative_integer", `{"coverage":{"billableCalls":1},"usage":{"total":-1}}`, "-1", 1, 1},
		{"fractional_number", `{"coverage":{"billableCalls":1},"usage":{"total":7.5}}`, "8", 1, 1},
		{"numeric_string_billable", `{"coverage":{"billableCalls":"1"},"usage":{"total":7}}`, "7", 1, 1},
		{"object_billable", `{"coverage":{"billableCalls":{"a":1}},"usage":{"total":7}}`, "7", 1, 1},
		{"array_billable", `{"coverage":{"billableCalls":[1,2]},"usage":{"total":7}}`, "7", 1, 1},
	}
	insert := func(sid, document string) {
		_, e := db.ExecContext(ctx, "INSERT INTO "+table+" (session_id,org_id,space_id,agent_uuid,agent_product,user_id,start_ns,end_ns,status,row_json) VALUES (?,?,?,?,?,?,?,?,?,?)", sid, "org", "space", "agent", "product", "owner", 100000000, 101000000, 1, document)
		require.NoError(t, e)
	}
	for _, fixture := range fixtures {
		insert(fixture.name, fixture.document)
	}
	// Aggregation must retain tool turns, and one unknown billable turn must
	// disqualify the complete session even when another turn has known usage.
	insert("mixed_known_tool", fixtures[0].document)
	insert("mixed_known_tool", fixtures[7].document)
	insert("mixed_unknown", fixtures[1].document)
	insert("mixed_unknown", fixtures[2].document)
	insert("mixed_large", fixtures[14].document)
	insert("mixed_large", fixtures[14].document)
	p := sessionReadModelTestParams()
	threshold := int64(0)
	p.MinTotalTokens = &threshold
	for _, optimized := range []bool{true} {
		t.Run(fmt.Sprintf("optimized_%t", optimized), func(t *testing.T) {
			store := &oceanBaseStore{db: db}
			query, args, e := store.sessionGroupSQL("org", table, p, "")
			require.NoError(t, e)
			// Project the actual production aggregation inputs without HAVING
			// so unknownness and exact totals are checked for all fixture rows.
			canonicalEnd := strings.Index(query, ") canonical GROUP BY")
			require.Positive(t, canonicalEnd)
			innerStart := strings.Index(query, " FROM (") + len(" FROM (")
			inner := query[innerStart:canonicalEnd]
			tx, e := store.beginReadModel(ctx, true)
			require.NoError(t, e)
			defer tx.Rollback()
			rows, e := scanReadModelRows(ctx, tx, "SELECT session_id,COUNT(*) AS trace_count,CAST(SUM(trace_tokens) AS CHAR) AS total_tokens,SUM(billable) AS billable_count,COUNT(trace_tokens) AS known_count FROM ("+inner+") canonical GROUP BY BINARY session_id,session_id", args[:len(args)-1]...)
			require.NoError(t, e)
			require.Len(t, rows, 23)
			byID := map[string]map[string]any{}
			for _, row := range rows {
				byID[row["session_id"].(string)] = row
			}
			for _, fixture := range fixtures {
				row := byID[fixture.name]
				require.NotNil(t, row, fixture.name)
				if fixture.total == "" {
					require.Nil(t, row["total_tokens"], fixture.name)
				} else {
					require.Equal(t, fixture.total, row["total_tokens"], fixture.name)
				}
				known, e := asInt64(row["known_count"])
				require.NoError(t, e)
				require.EqualValues(t, fixture.known, known, fixture.name)
				billable, e := asInt64(row["billable_count"])
				require.NoError(t, e)
				require.EqualValues(t, fixture.billable, billable, fixture.name)
			}
			require.Equal(t, "0", byID["mixed_known_tool"]["total_tokens"])
			require.Equal(t, "27", byID["mixed_unknown"]["total_tokens"])
			require.Equal(t, "36893488147419103230", byID["mixed_large"]["total_tokens"], "DECIMAL aggregation must remain exact above uint64")
			for sid, want := range map[string]int64{"mixed_known_tool": 2, "mixed_unknown": 1, "mixed_large": 2} {
				known, e := asInt64(byID[sid]["known_count"])
				require.NoError(t, e)
				require.Equal(t, want, known)
			}
			selected, e := scanReadModelRows(ctx, tx, query+" ORDER BY BINARY session_id", args...)
			require.NoError(t, e)
			ids := []string{}
			for _, row := range selected {
				ids = append(ids, row["session_id"].(string))
			}
			require.Equal(t, []string{"above_js_safe_integer", "array_billable", "fractional_number", "known_integer", "known_zero", "max_uint64", "mixed_known_tool", "mixed_large", "numeric_string_billable", "object_billable"}, ids)
			require.NoError(t, tx.Commit())
		})
	}
}
