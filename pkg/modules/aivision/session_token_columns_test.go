package aivision

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/factory"
	"github.com/SigNoz/signoz/pkg/telemetrystore"
)

func TestSessionTokenGeneratedMaintenanceFilesDoNotDrift(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("test source path unavailable")
	}
	deploy := filepath.Join(filepath.Dir(filename), "../../../deploy/oceanbase")
	normalize := func(value string) string {
		lines := []string{}
		for _, line := range strings.Split(value, "\n") {
			if !strings.HasPrefix(strings.TrimSpace(line), "--") {
				lines = append(lines, line)
			}
		}
		value = strings.TrimSuffix(strings.TrimSpace(strings.Join(lines, "\n")), ";")
		var out strings.Builder
		quoted := false
		for _, character := range value {
			if character == '\'' {
				quoted = !quoted
			}
			if !quoted && (character == ' ' || character == '\t' || character == '\n' || character == '\r') {
				continue
			}
			out.WriteRune(character)
		}
		return out.String()
	}
	ddl, err := sessionTokenGeneratedDDL("signoz")
	if err != nil {
		t.Fatal(err)
	}
	forward, err := os.ReadFile(filepath.Join(deploy, "session-token-generated-v1.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if normalize(string(forward)) != normalize(ddl) {
		t.Fatal("maintenance migration differs from versioned helper expressions/DDL")
	}
	backward, err := os.ReadFile(filepath.Join(deploy, "session-token-generated-v1-rollback.sql"))
	if err != nil {
		t.Fatal(err)
	}
	want := "ALTER TABLE `signoz_trace_projections` DROP COLUMN " + sessionTokenGeneratedValueColumn + ", DROP COLUMN " + sessionTokenGeneratedBillableColumn
	if normalize(string(backward)) != normalize(want) {
		t.Fatal("rollback must only remove the two optional versioned generated columns")
	}
}

func TestSessionTokenGeneratedDDLIsExplicitAndExact(t *testing.T) {
	ddl, err := sessionTokenGeneratedDDL("test")
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"ALTER TABLE `test_trace_projections`", sessionTokenGeneratedValueColumn + " DECIMAL(65,0) GENERATED ALWAYS AS (" + sessionTokenGeneratedValueExpr + ") STORED", sessionTokenGeneratedBillableColumn + " TINYINT GENERATED ALWAYS AS (" + sessionTokenGeneratedBillableExpr + ") STORED"} {
		if !strings.Contains(ddl, expected) {
			t.Fatalf("DDL does not retain expected versioned definition: %s", expected)
		}
	}
	for _, prefix := range []string{"", "1bad", "bad`table", "bad;table", strings.Repeat("a", 33)} {
		if _, err := sessionTokenGeneratedDDL(prefix); err == nil {
			t.Fatalf("unsafe prefix accepted: %q", prefix)
		}
	}
}

func TestSessionTokenGeneratedExpressionMetadata(t *testing.T) {
	for _, actual := range []string{sessionTokenGeneratedValueExpr, "(" + sessionTokenGeneratedValueExpr + ")", sessionTokenGeneratedStoredValueExpr, strings.ReplaceAll(sessionTokenGeneratedStoredValueExpr, "JSON_EXTRACT", "json_extract")} {
		if !generatedExpressionMatches(actual, sessionTokenGeneratedValueExpr, sessionTokenGeneratedStoredValueExpr) {
			t.Fatalf("known exact metadata expression rejected: %s", actual)
		}
	}
	for _, actual := range []string{
		strings.ReplaceAll(sessionTokenGeneratedStoredValueExpr, "$.usage.total", "$.usage.input"),
		strings.ReplaceAll(sessionTokenGeneratedStoredValueExpr, "$.coverage.billableCalls", "$.coverage.billablecalls"),
		strings.ReplaceAll(sessionTokenGeneratedStoredValueExpr, "then 0", "then NULL"),
		strings.ReplaceAll(sessionTokenGeneratedStoredValueExpr, "number(65,0)", "number(20,0)"),
		strings.ReplaceAll(sessionTokenGeneratedStoredValueExpr, " > 0", " = 0"),
		sessionTokenGeneratedValueExpr + " OR 1=1",
		"CASE WHEN TRUE THEN 0 ELSE NULL END",
		"JSON_EXTRACT(row_json,'unterminated)",
		"/* ignored */ " + sessionTokenGeneratedValueExpr,
	} {
		if generatedExpressionMatches(actual, sessionTokenGeneratedValueExpr, sessionTokenGeneratedStoredValueExpr) {
			t.Fatalf("different or untrusted expression accepted: %s", actual)
		}
	}
}

func generatedCapabilityRows(valueType, valueExtra string, valueExpr any, billableType, billableExtra string, billableExpr any) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"COLUMN_NAME", "COLUMN_TYPE", "EXTRA", "GENERATION_EXPRESSION"}).
		AddRow(sessionTokenGeneratedValueColumn, valueType, valueExtra, valueExpr).
		AddRow(sessionTokenGeneratedBillableColumn, billableType, billableExtra, billableExpr)
}

func TestSessionTokenGeneratedCapabilityReadsExactOptionalDefinitions(t *testing.T) {
	const query = "SELECT COLUMN_NAME,COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME IN (?,?)"
	for _, tc := range []struct {
		name   string
		rows   *sqlmock.Rows
		ready  bool
		reason string
	}{
		{"source form", generatedCapabilityRows("decimal(65,0)", "STORED GENERATED", sessionTokenGeneratedValueExpr, "tinyint", "STORED GENERATED", sessionTokenGeneratedBillableExpr), true, "ready"},
		{"observed engine form", generatedCapabilityRows("decimal(65,0)", "STORED GENERATED", sessionTokenGeneratedStoredValueExpr, "tinyint(4)", "STORED GENERATED", sessionTokenGeneratedStoredBillableExpr), true, "ready"},
		{"missing all", sqlmock.NewRows([]string{"COLUMN_NAME", "COLUMN_TYPE", "EXTRA", "GENERATION_EXPRESSION"}), false, "session_token_columns_missing"},
		{"missing one", sqlmock.NewRows([]string{"COLUMN_NAME", "COLUMN_TYPE", "EXTRA", "GENERATION_EXPRESSION"}).AddRow(sessionTokenGeneratedValueColumn, "decimal(65,0)", "STORED GENERATED", sessionTokenGeneratedValueExpr), false, "session_token_columns_missing"},
		{"ordinary value", generatedCapabilityRows("decimal(65,0)", "", nil, "tinyint", "STORED GENERATED", sessionTokenGeneratedBillableExpr), false, "session_token_columns_mismatch"},
		{"virtual value", generatedCapabilityRows("decimal(65,0)", "VIRTUAL GENERATED", sessionTokenGeneratedValueExpr, "tinyint", "STORED GENERATED", sessionTokenGeneratedBillableExpr), false, "session_token_columns_mismatch"},
		{"lossy type", generatedCapabilityRows("double", "STORED GENERATED", sessionTokenGeneratedValueExpr, "tinyint", "STORED GENERATED", sessionTokenGeneratedBillableExpr), false, "session_token_columns_mismatch"},
		{"wrong value expression", generatedCapabilityRows("decimal(65,0)", "STORED GENERATED", "0", "tinyint", "STORED GENERATED", sessionTokenGeneratedBillableExpr), false, "session_token_columns_mismatch"},
		{"wrong billable expression", generatedCapabilityRows("decimal(65,0)", "STORED GENERATED", sessionTokenGeneratedValueExpr, "tinyint", "STORED GENERATED", "1"), false, "session_token_columns_mismatch"},
		{"unsigned billable", generatedCapabilityRows("decimal(65,0)", "STORED GENERATED", sessionTokenGeneratedValueExpr, "tinyint unsigned", "STORED GENERATED", sessionTokenGeneratedBillableExpr), false, "session_token_columns_mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectBegin()
			tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
			if err != nil {
				t.Fatal(err)
			}
			mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs("test_trace_projections", sessionTokenGeneratedValueColumn, sessionTokenGeneratedBillableColumn).WillReturnRows(tc.rows)
			store := &oceanBaseStore{cfg: config{TablePrefix: "test"}}
			ready, reason, err := store.sessionTokenGeneratedCapability(context.Background(), tx)
			if err != nil || ready != tc.ready || reason != tc.reason {
				t.Fatalf("got (%v,%q,%v), want (%v,%q,nil)", ready, reason, err, tc.ready, tc.reason)
			}
			mock.ExpectRollback()
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSessionTokenGeneratedCapabilityDoesNotHideInspectionFailures(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rows       *sqlmock.Rows
		queryError error
	}{
		{"permission or DB failure", nil, errors.New("metadata permission denied")},
		{"scan failure", sqlmock.NewRows([]string{"COLUMN_NAME", "COLUMN_TYPE", "EXTRA", "GENERATION_EXPRESSION"}).AddRow(sessionTokenGeneratedValueColumn, nil, "STORED GENERATED", sessionTokenGeneratedValueExpr), nil},
		{"iteration failure", generatedCapabilityRows("decimal(65,0)", "STORED GENERATED", sessionTokenGeneratedValueExpr, "tinyint", "STORED GENERATED", sessionTokenGeneratedBillableExpr).RowError(1, errors.New("read interrupted")), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectBegin()
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			expect := mock.ExpectQuery("SELECT COLUMN_NAME,COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", sessionTokenGeneratedValueColumn, sessionTokenGeneratedBillableColumn)
			if tc.queryError != nil {
				expect.WillReturnError(tc.queryError)
			} else {
				expect.WillReturnRows(tc.rows)
			}
			store := &oceanBaseStore{cfg: config{TablePrefix: "test"}}
			ready, reason, err := store.sessionTokenGeneratedCapability(context.Background(), tx)
			if err == nil || ready || reason != "" {
				t.Fatalf("inspection failure silently downgraded: %v %q %v", ready, reason, err)
			}
			mock.ExpectRollback()
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Exercise same-transaction automatic capability and all five actual
// group-query consumers. Empty pages avoid unrelated summary fixtures while
// retaining count, page and the three independently selected facet builders.
func TestSessionTokenGeneratedActualPagePath(t *testing.T) {
	zero, positive := int64(0), int64(100)
	for _, tc := range []struct {
		name, flag, capability, strategy, reason string
		threshold                                *int64
	}{
		{"enabled_without_token", "true", "not_queried", "not_requested", "", nil},
		{"ready_zero", "true", "ready", "generated_columns", "ready", &zero},
		{"ready_positive", "true", "ready", "generated_columns", "ready", &positive},
		{"missing_falls_back_to_json", "true", "missing", "json_table", "session_token_columns_missing", &zero},
		{"mismatch_falls_back_to_json", "true", "mismatch", "json_table", "session_token_columns_mismatch", &zero},
		{"permission_is_error", "true", "permission", "", "", &zero},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SIGNOZ_AIVISION_SESSION_TOKEN_QUERY_OPTIMIZATION_ENABLED", "true")
			t.Setenv("SIGNOZ_AIVISION_SESSION_TOKEN_GENERATED_COLUMNS_ENABLED", tc.flag)
			t.Setenv("SIGNOZ_AIVISION_SESSION_PAGE_BATCH_ENABLED", "false")
			t.Setenv("SIGNOZ_AIVISION_TRACE_READ_MODEL_AUTHORITATIVE_INGEST", "true")
			groupQueries := 0
			matcher := sqlmock.QueryMatcherFunc(func(expected, actual string) error {
				if expected == "group_consumer" || expected == "window_consumer" {
					wantGenerated := expected == "group_consumer" && tc.strategy == "generated_columns"
					wantJSONTable := expected == "group_consumer" && tc.strategy == "json_table"
					generatedValue := strings.Contains(actual, "p."+sessionTokenGeneratedValueColumn)
					generatedBillable := strings.Contains(actual, "p."+sessionTokenGeneratedBillableColumn)
					jsonTable := strings.Contains(actual, "JSON_TABLE(p.metadata_json_v1,")
					if generatedValue != wantGenerated || generatedBillable != wantGenerated || jsonTable != wantJSONTable {
						return fmt.Errorf("wrong group strategy: generatedValue=%t billable=%t JSON_TABLE=%t", generatedValue, generatedBillable, jsonTable)
					}
					if wantGenerated && strings.Contains(actual, "JSON_EXTRACT(p.row_json,'$.usage.total')") {
						return errors.New("generated group still parses row_json Tokens")
					}
					if expected == "group_consumer" {
						groupQueries++
						if tc.threshold != nil && !strings.Contains(actual, "COUNT(trace_tokens)=COUNT(*) AND SUM(billable)>0 AND SUM(trace_tokens)>=?") {
							return errors.New("whole-group knownness/billable/threshold predicates lost")
						}
					}
					return nil
				}
				if !strings.Contains(actual, expected) {
					return fmt.Errorf("missing query fragment: %s", expected)
				}
				return nil
			})
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			store := NewModule(factory.ProviderSettings{}, nil, nil, telemetrystore.Config{}).store
			store.db, store.cfg.TablePrefix = db, "test"
			params := sessionReadModelTestParams()
			params.MinTotalTokens = tc.threshold
			mock.ExpectBegin()
			expectProjectionMaintenance(mock, true, false)
			expectProjectionSummaryCapability(mock)
			mock.ExpectQuery("SELECT complete FROM").WithArgs("org", "space").WillReturnRows(sqlmock.NewRows([]string{"complete"}).AddRow(true))
			mock.ExpectQuery("SELECT COALESCE").WillReturnRows(sqlmock.NewRows([]string{"stale", "boundary", "incomplete_content"}).AddRow(0, 0, 0))
			failure := errors.New("metadata permission denied")
			if tc.capability != "not_queried" {
				expect := mock.ExpectQuery("SELECT COLUMN_NAME,COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", sessionTokenGeneratedValueColumn, sessionTokenGeneratedBillableColumn)
				switch tc.capability {
				case "permission":
					expect.WillReturnError(failure)
				case "missing":
					expect.WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "COLUMN_TYPE", "EXTRA", "GENERATION_EXPRESSION"}))
				case "mismatch":
					expect.WillReturnRows(generatedCapabilityRows("decimal(65,0)", "STORED GENERATED", "0", "tinyint", "STORED GENERATED", sessionTokenGeneratedBillableExpr))
				default:
					expect.WillReturnRows(generatedCapabilityRows("decimal(65,0)", "STORED GENERATED", sessionTokenGeneratedStoredValueExpr, "tinyint(4)", "STORED GENERATED", sessionTokenGeneratedStoredBillableExpr))
				}
			}
			if tc.capability == "permission" {
				mock.ExpectRollback()
			} else {
				mock.ExpectQuery("group_consumer").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
				mock.ExpectQuery("window_consumer").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(4))
				mock.ExpectQuery("group_consumer").WillReturnRows(sqlmock.NewRows([]string{"session_id", "trace_count", "last_activity"}))
				for range 3 {
					mock.ExpectQuery("group_consumer").WillReturnRows(sqlmock.NewRows([]string{"value", "count"}))
				}
				mock.ExpectCommit()
			}
			result, err := store.sessionReadModelPage(context.Background(), "org", params)
			if store.sessionTokenGeneratedReady {
				t.Fatal("request capability polluted the shared store")
			}
			if tc.capability == "permission" {
				if !errors.Is(err, failure) || result != nil || groupQueries != 0 {
					t.Fatalf("permission failure fell through to a data query: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				meta := result.(map[string]any)["meta"].(map[string]any)
				if meta["sessionTokenQueryStrategy"] != tc.strategy {
					t.Fatalf("strategy diagnostic mismatch: %v", meta["sessionTokenQueryStrategy"])
				}
				if tc.reason == "" {
					if _, present := meta["sessionTokenGeneratedColumns"]; present {
						t.Fatal("disabled or absent Token unexpectedly inspected optional columns")
					}
				} else if meta["sessionTokenGeneratedColumns"] != tc.reason {
					t.Fatalf("capability reason mismatch: %v", meta["sessionTokenGeneratedColumns"])
				}
				if groupQueries != 5 {
					t.Fatalf("expected five identical-strategy group consumers, got %d", groupQueries)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
