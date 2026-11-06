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

	"github.com/SigNoz/signoz/pkg/instrumentation/instrumentationtest"
	"github.com/SigNoz/signoz/pkg/telemetrystore"
	"github.com/SigNoz/signoz/pkg/telemetrystore/oceanbasetelemetrystore"
	"github.com/stretchr/testify/require"
)

func TestNativeScopeMetadataSurvivesNormalizedReadPaths(t *testing.T) {
	for _, path := range []string{"detail", "source", "page"} {
		t.Run(path, func(t *testing.T) {
			attrs := map[string]any{"powercontext.operation.name": "prepare_context", "aivision.otlp.scope.name": "unrelated"}
			span := map[string]any{"scope_name": "powercontext.server", "scope_version": "1.5.0", "attributes": attrs}
			if path == "page" {
				rows := tracePageTestRows(t)
				raw := rows[2]["result"].(map[string]any)["spans"].([]any)[0].(map[string]any)
				raw["scope_name"], raw["scope_version"] = "powercontext.server", "1.5.0"
				result, err := decodeTracePage(rows, nativeListParams{Page: 1, Limit: 20, SpaceID: "space-a"})
				require.NoError(t, err)
				span = result.(map[string]any)["data"].([]tracePageItem)[0].Spans[0]
			} else if path == "source" {
				require.NoError(t, decodeWindowSpan(span))
				require.NoError(t, compactNativeSpan(span))
			} else {
				normalizeNativeSpan(span)
			}
			normalized := span["attributes"].(map[string]any)
			require.Equal(t, "powercontext.server", normalized["aivision.otlp.scope.name"])
			require.Equal(t, "1.5.0", normalized["aivision.otlp.scope.version"])
		})
	}
}

func TestNativeScopeMetadataKeepsLegacyFallbackWithoutStoredScope(t *testing.T) {
	row := map[string]any{"scope_name": "", "scope_version": "", "attributes": map[string]any{"aivision.otlp.scope.name": "powercontext.client"}}
	normalizeNativeSpan(row)
	require.Equal(t, "powercontext.client", row["attributes"].(map[string]any)["aivision.otlp.scope.name"])
}

// The producer's standard OTLP instrumentation scope lives in storage columns,
// not in application span attributes. Exercise its real SQL candidate/read path.
func TestOceanBaseNativeMemoryUsesStoredInstrumentationScope(t *testing.T) {
	dsn := os.Getenv("SIGNOZ_TEST_OCEANBASE_DSN")
	if dsn == "" {
		t.Skip("set real database DSN")
	}
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	defer db.Close()
	prefix := fmt.Sprintf("sn_scope_%d", time.Now().UnixNano())
	table := "`" + prefix + "_traces`"
	_, err = db.Exec("CREATE TABLE " + table + " LIKE signoz_traces")
	require.NoError(t, err)
	defer db.Exec("DROP TABLE " + table)
	fixture := tracePageIntegrationFixture{db: db, table: table, org: "scope-test", start: time.Now()}
	root, _ := fixture.pair("scope-space", 1)
	root.attributes = map[string]any{"powercontext.operation.name": "prepare_context", "powercontext.operation.unit": "application", "powercontext.operation.outcome": "success"}
	fixture.insert(t, []tracePageIntegrationSpan{root})
	_, err = db.Exec("UPDATE "+table+" SET scope_name=?,scope_version=?", "powercontext.server", "1.5.0")
	require.NoError(t, err)
	cfg := telemetrystore.NewConfigFactory().New().(telemetrystore.Config)
	cfg.Provider = "oceanbase"
	cfg.OceanBase.DSN, cfg.OceanBase.TablePrefix = dsn, prefix
	settings := instrumentationtest.New().ToProviderSettings()
	backend, err := oceanbasetelemetrystore.New(context.Background(), settings, cfg)
	require.NoError(t, err)
	defer backend.(interface{ Close() error }).Close()
	store := NewModule(settings, backend, nil, cfg).store
	p := nativeListParams{From: fixture.start.Add(-time.Minute).UnixMilli(), To: fixture.start.Add(time.Minute).UnixMilli(), SpaceID: "scope-space", Page: 1, Limit: 50}
	candidates, err := store.listMemorySpans(context.Background(), fixture.org, p)
	require.NoError(t, err)
	require.Len(t, candidates.Spans, 1, "standard OTLP scope must identify the actual native operation")
	require.Equal(t, "powercontext.server", candidates.Spans[0]["attributes"].(map[string]any)["aivision.otlp.scope.name"])
	detail, err := store.getTraceDetail(context.Background(), fixture.org, root.traceID, p)
	require.NoError(t, err)
	require.Equal(t, "1.5.0", detail.Spans[0]["attributes"].(map[string]any)["aivision.otlp.scope.version"])
	var raw string
	require.NoError(t, db.QueryRow("SELECT "+tracePageSpanJSON()+" FROM "+table+" b").Scan(&raw))
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var pageRow map[string]any
	require.NoError(t, decoder.Decode(&pageRow))
	require.NoError(t, decodeWindowSpan(pageRow))
	require.Equal(t, "powercontext.server", pageRow["attributes"].(map[string]any)["aivision.otlp.scope.name"])
}

func TestNativeScopeMetadataFitsReservedWireAllowance(t *testing.T) {
	row := map[string]any{"scope_name": strings.Repeat("\u2029", 255), "scope_version": strings.Repeat("\x01", 255), "attributes": map[string]any{}}
	normalizeNativeSpan(row)
	encoded, err := json.Marshal(row)
	require.NoError(t, err)
	require.LessOrEqual(t, len(encoded), nativeScopeWireByteAllowance)
}
