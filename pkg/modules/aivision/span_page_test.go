package aivision

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	cmock "github.com/SigNoz/clickhouse-go-mock"
	"github.com/SigNoz/signoz/pkg/telemetrystore"
	"github.com/stretchr/testify/require"
)

func candidateTestRows(columns []cmock.ColumnType, data [][]any) telemetrystore.Rows {
	return telemetrystore.AdaptClickHouseRows(telemetrystore.WrapRows(cmock.NewRows(columns, data)), nil)
}

func TestCandidateSpanHandlersUseOneQuery(t *testing.T) {
	for _, kind := range []string{"mcp", "skill"} {
		t.Run(kind, func(t *testing.T) {
			store := &tracePageUnitStore{rows: candidateTestRows(
				[]cmock.ColumnType{
					{Name: "candidate_total", Type: "Int64"},
					{Name: "trace_id", Type: "String"},
					{Name: "span_id", Type: "String"},
					{Name: "resource_attributes", Type: "String"},
				},
				[][]any{{int64(501), "trace-a", "span-a", `{"aivision.space.id":"space-a"}`}},
			)}
			handler := tracePageUnitHandler(store)
			request := tracePageUnitRequest(url.Values{
				"spans_only": {"true"}, "page": {"2"}, "limit": {"500"},
				"user": {"subject-a"}, "agent_product": {"Codex"},
			}, true)
			response := httptest.NewRecorder()
			if kind == "mcp" {
				handler.McpTools(response, request)
			} else {
				handler.SkillSpans(response, request)
			}
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			require.Equal(t, 1, store.queries)
			for _, value := range []any{tracePageUnitOrg, "space-a", "subject-a", "Codex", 500} {
				require.Contains(t, store.args, value)
			}
			require.Contains(t, response.Body.String(), `"totalItems":501`)
			require.NotContains(t, response.Body.String(), "candidate_total")
		})
	}
}

type candidatePageTestStore struct {
	telemetrystore.TelemetryStore
	queries []string
	results []telemetrystore.Rows
}

func (s *candidatePageTestStore) QueryContext(_ context.Context, query string, _ ...any) (telemetrystore.Rows, error) {
	s.queries = append(s.queries, query)
	result := s.results[0]
	s.results = s.results[1:]
	return result, nil
}

func TestCandidateEmptyPagesPreserveTotals(t *testing.T) {
	for _, page := range []int{1, 3} {
		t.Run(strconv.Itoa(page), func(t *testing.T) {
			store := &candidatePageTestStore{results: []telemetrystore.Rows{
				candidateTestRows([]cmock.ColumnType{{Name: "candidate_total", Type: "Int64"}}, nil),
			}}
			if page > 1 {
				store.results = append(store.results, candidateTestRows(
					[]cmock.ColumnType{{Name: "total_items", Type: "Int64"}}, [][]any{{int64(501)}},
				))
			}
			backend := tracePageUnitHandler(nil).backend
			backend.telemetryStore = store
			params := nativeListParams{Page: page, Limit: 500}
			result, err := backend.listCandidateSpans(context.Background(), params, "space_id = ?", []any{"space-a"})
			require.NoError(t, err)
			require.Empty(t, result.Spans)
			require.Contains(t, store.queries[0], "COUNT(*) OVER () AS candidate_total")
			require.Contains(t, store.queries[0], "timestamp DESC, trace_id DESC, span_id DESC")
			if page == 1 {
				require.Len(t, store.queries, 1)
				require.Zero(t, result.SpanMeta.TotalItems)
			} else {
				require.Len(t, store.queries, 2)
				require.Equal(t, 501, result.SpanMeta.TotalItems)
			}
		})
	}
}
