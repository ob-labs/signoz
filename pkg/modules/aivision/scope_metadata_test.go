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
			switch path {
			case "page":
				rows := tracePageTestRows(t)
				raw := rows[2]["result"].(map[string]any)["spans"].([]any)[0].(map[string]any)
				raw["scope_name"], raw["scope_version"] = "powercontext.server", "1.5.0"
				result, err := decodeTracePage(rows, nativeListParams{Page: 1, Limit: 20, SpaceID: "space-a"})
				require.NoError(t, err)
				span = result.(map[string]any)["data"].([]tracePageItem)[0].Spans[0]
			case "source":
				require.NoError(t, decodeWindowSpan(span))
				require.NoError(t, compactNativeSpan(span))
			default:
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
	defer func() {
		_, dropErr := db.Exec("DROP TABLE " + table)
		require.NoError(t, dropErr)
	}()
	fixture := tracePageIntegrationFixture{db: db, table: table, org: "scope-test", start: time.Now()}
	root, _ := fixture.pair("scope-space", 1)
	root.attributes = map[string]any{"powercontext.operation.name": "prepare_context", "powercontext.operation.unit": "application", "powercontext.operation.outcome": "success"}
	capture, _ := fixture.pair("scope-space", 2)
	capture.attributes = map[string]any{"powercontext.operation.name": "capture_content_source", "powercontext.operation.unit": "dependency"}
	binding, _ := fixture.pair("scope-space", 3)
	binding.attributes = map[string]any{"powercontext.operation.name": "resolve_scope_binding", "powercontext.operation.unit": "application"}
	connector, _ := fixture.pair("scope-space", 4)
	connector.attributes = map[string]any{"powercontext.operation.name": "mcp.tools.call", "powercontext.operation.unit": "transport", "powercontext.transport": "mcp"}
	stage, _ := fixture.pair("scope-space", 5)
	stage.attributes = map[string]any{"powercontext.operation.name": "memory.capture", "powercontext.operation.unit": "stage"}
	fixture.insert(t, []tracePageIntegrationSpan{root, capture, binding, connector, stage})
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
	require.Len(t, candidates.Spans, 3, "recall/capture and MCP ownership context must survive the bounded native scan")
	operations := []string{}
	for _, candidate := range candidates.Spans {
		operations = append(operations, candidate["attributes"].(map[string]any)["powercontext.operation.name"].(string))
	}
	require.ElementsMatch(t, []string{"prepare_context", "capture_content_source", "mcp.tools.call"}, operations)
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

	t.Run("memory_scan_retains_mcp_owner_context_without_widening_discovery", func(t *testing.T) {
		markers := []map[string]any{
			{"gen_ai.span.kind": "MCP", "agentic.tool.name": "powercontext.search_memory"},
			{"gen_ai.tool.name": "mcp__powercontext__search_memory"},
			{"agentic.tool.type": " MCP ", "agentic.tool.name": "powercontext.search_memory"},
			{"agentic.tool.raw_name": "  mcp__powercontext__search_memory  "},
			{"gen_ai.span.kind": "TOOL", "agentic.tool.name": "powercontext.search_memory"},
		}
		wantIDs := []string{}
		for index, attributes := range markers {
			owner, _ := fixture.pair("mcp-owner-space", 10+index)
			owner.attributes = attributes
			child, _ := fixture.pair("mcp-owner-space", 20+index)
			child.traceID, child.parent = owner.traceID, owner.spanID
			child.attributes = map[string]any{
				"aivision.otlp.scope.name": "powercontext.client", "powercontext.operation.name": "prepare_context", "powercontext.operation.unit": "dependency",
			}
			fixture.insert(t, []tracePageIntegrationSpan{owner, child})
			wantIDs = append(wantIDs, child.spanID)
			if index < len(markers)-1 {
				wantIDs = append(wantIDs, owner.spanID)
			}
		}
		ownerParams := p
		ownerParams.SpaceID = "mcp-owner-space"
		observed, err := store.listMemorySpans(context.Background(), fixture.org, ownerParams)
		require.NoError(t, err)
		gotIDs := []string{}
		for _, candidate := range observed.Spans {
			gotIDs = append(gotIDs, candidate["span_id"].(string))
		}
		require.ElementsMatch(t, wantIDs, gotIDs, "explicit MCP owners must accompany native children; ordinary tools must stay excluded")
		var discoveryCount int
		require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE space_id=? AND "+mcpToolPredicateSQL(), "mcp-owner-space").Scan(&discoveryCount))
		require.Zero(t, discoveryCount, "memory-only ownership context must not change MCP discovery")
	})
}

func TestNativeScopeMetadataFitsReservedWireAllowance(t *testing.T) {
	row := map[string]any{"scope_name": strings.Repeat("\u2029", 255), "scope_version": strings.Repeat("\x01", 255), "attributes": map[string]any{}}
	normalizeNativeSpan(row)
	encoded, err := json.Marshal(row)
	require.NoError(t, err)
	require.LessOrEqual(t, len(encoded), nativeScopeWireByteAllowance)
}
