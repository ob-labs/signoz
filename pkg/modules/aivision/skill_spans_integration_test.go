package aivision

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SigNoz/signoz/pkg/instrumentation/instrumentationtest"
	"github.com/SigNoz/signoz/pkg/telemetrystore"
	"github.com/SigNoz/signoz/pkg/telemetrystore/oceanbasetelemetrystore"
	"github.com/SigNoz/signoz/pkg/valuer"
	"github.com/stretchr/testify/require"
)

// Exercise the production handler and SQL in a uniquely named copy of the
// traces table. The existing schema and telemetry are never modified.
func TestOceanBaseSkillSpans(t *testing.T) {
	dsn := os.Getenv("SIGNOZ_TEST_OCEANBASE_DSN")
	if dsn == "" {
		t.Skip("set SIGNOZ_TEST_OCEANBASE_DSN for a real OceanBase integration test")
	}
	ctx := context.Background()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	prefix := "sn_skill_" + strings.ReplaceAll(valuer.GenerateUUID().StringValue(), "-", "")[:12]
	table := "`" + prefix + "_traces`"
	_, err = db.ExecContext(ctx, "CREATE TABLE "+table+" LIKE `signoz_traces`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := db.ExecContext(context.Background(), "DROP TABLE "+table)
		require.NoError(t, err)
	})
	cfg := telemetrystore.NewConfigFactory().New().(telemetrystore.Config)
	cfg.Provider = "oceanbase"
	cfg.OceanBase.DSN = dsn
	cfg.OceanBase.TablePrefix = prefix
	settings := instrumentationtest.New().ToProviderSettings()
	backend, err := oceanbasetelemetrystore.New(ctx, settings, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backend.(interface{ Close() error }).Close()) })
	h := NewHandler(NewModule(settings, backend, nil, cfg))
	lastWire := ""
	fixture := tracePageIntegrationFixture{
		db: db, table: table, org: valuer.GenerateUUID().StringValue(),
		start: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), lastWire: &lastWire,
	}
	query := func(t *testing.T, scope string, extra url.Values) map[string]any {
		t.Helper()
		status, payload := fixture.call(t, h.SkillSpans, scope, extra)
		require.Equal(t, http.StatusOK, status, payload)
		return payload["data"].(map[string]any)
	}

	t.Run("native_name_without_argument_path", func(t *testing.T) {
		span, _ := fixture.pair("native-name", 1)
		span.attributes = map[string]any{"agentic.skill.name": "deploy"}
		fixture.insert(t, []tracePageIntegrationSpan{span})
		result := query(t, "native-name", nil)
		require.EqualValues(t, 1, result["spanMeta"].(map[string]any)["totalItems"])
		spans := result["spans"].([]any)
		require.Len(t, spans, 1)
		require.Equal(t, "deploy", spans[0].(map[string]any)["attributes"].(map[string]any)["agentic.skill.name"])
	})

	t.Run("stable_tied_timestamp_pages", func(t *testing.T) {
		const count, limit = 501, 200
		spans := make([]tracePageIntegrationSpan, 0, count)
		for index := 0; index < count; index++ {
			span, _ := fixture.pair("tied-pages", index)
			span.traceID = fmt.Sprintf("%032x", index/2)
			span.attributes = map[string]any{"agentic.tool.type": "skill", "agentic.tool.name": "deploy"}
			spans = append(spans, span)
		}
		fixture.insert(t, spans)
		seen := map[string]bool{}
		for page := 1; page <= 3; page++ {
			result := query(t, "tied-pages", url.Values{"page": {strconv.Itoa(page)}, "limit": {strconv.Itoa(limit)}})
			meta := result["spanMeta"].(map[string]any)
			require.EqualValues(t, count, meta["totalItems"])
			require.EqualValues(t, 3, meta["totalPages"])
			for _, value := range result["spans"].([]any) {
				span := value.(map[string]any)
				identity := span["trace_id"].(string) + "/" + span["span_id"].(string)
				require.False(t, seen[identity], "span repeated across pages")
				seen[identity] = true
			}
		}
		require.Len(t, seen, count)
	})
}
