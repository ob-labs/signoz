package aivision

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/types/authtypes"
	"github.com/stretchr/testify/require"
)

// Removing parameter validation must never allow callers to expand the source scope.
func TestSourcePageRejectsUnknownParametersBeforeReading(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	cfg := config{Enabled: true, QueryTimeout: time.Second, TablePrefix: "test"}
	h := &handler{backend: &oceanBaseStore{db: db, cfg: cfg}, cfg: cfg, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	r := httptest.NewRequest(http.MethodGet, "/api/v1/ai-vision/traces/read-model/source-page?space_id=space&trace_id=abc&from=1", nil)
	r = r.WithContext(authtypes.NewContextWithClaims(r.Context(), authtypes.Claims{OrgID: supplementTestOrg}))
	w := httptest.NewRecorder()
	h.ReadModel(w, r)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.NoError(t, mock.ExpectationsWereMet())
}

func expectSourcePageFence(mock sqlmock.Sqlmock, revision, generation int64, valid bool) {
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT enabled,generation,schema_version").WillReturnRows(sqlmock.NewRows([]string{"enabled", "generation", "schema_version"}).AddRow(true, generation, 1))
	mock.ExpectQuery("SELECT source_revision,min_timestamp,max_timestamp,user_id,agent_product,scope_valid").WithArgs("org", "org", "space", "space", "abc", "abc").WillReturnRows(sqlmock.NewRows([]string{"revision", "min", "max", "user", "product", "valid"}).AddRow(revision, 100, 200, "owner", "product", valid))
}

func expectSourceRawSize(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT COUNT.*SUM.*OCTET_LENGTH.*MAX.*OCTET_LENGTH").WillReturnRows(sqlmock.NewRows([]string{"count", "bytes", "max"}).AddRow(1, 18<<20, 18<<20))
}

// A single Span above the v1 limit must produce a bounded fragment instead of a decline.
func TestSourcePageFragmentsSpanLargerThanLegacyLimit(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	expectSourcePageFence(mock, 4, 7, true)
	expectSourceRawSize(mock)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\),COALESCE\\(SUM\\(OCTET_LENGTH").WillReturnRows(sqlmock.NewRows([]string{"count", "bytes"}).AddRow(1, 18<<20))
	mock.ExpectQuery("SELECT span_id,OCTET_LENGTH").WillReturnRows(sqlmock.NewRows([]string{"span_id", "total_bytes"}).AddRow("001", 18<<20))
	payload := []byte(strings.Repeat("x", 1280<<10))
	mock.ExpectQuery("SELECT 0 AS ordinal,span_id,SUBSTRING\\(CAST\\(JSON_OBJECT").WillReturnRows(sqlmock.NewRows([]string{"ordinal", "span_id", "fragment"}).AddRow(0, "001", payload))
	mock.ExpectCommit()
	result, err := s.traceSourcePage(context.Background(), "org", "space", "abc", "")
	require.NoError(t, err)
	page := result.(map[string]any)
	require.Equal(t, true, page["supported"])
	require.Equal(t, false, page["complete"])
	require.EqualValues(t, 18<<20, page["source_bytes"])
	fragments := page["fragments"].([]sourcePageFragment)
	require.Len(t, fragments, 1)
	require.EqualValues(t, 0, fragments[0].Offset)
	require.EqualValues(t, 18<<20, fragments[0].TotalBytes)
	decoded, err := base64.StdEncoding.DecodeString(fragments[0].DataBase64)
	require.NoError(t, err)
	require.Equal(t, payload, decoded)
	wire, err := json.Marshal(page)
	require.NoError(t, err)
	require.LessOrEqual(t, len(wire), 2<<20)
	require.NotEmpty(t, page["next_cursor"])
	require.NoError(t, mock.ExpectationsWereMet())
}

// A revision or generation change between requests must invalidate the entire reconstruction.
func TestSourcePageCursorRejectsChangedFences(t *testing.T) {
	for _, tc := range []struct {
		revision, generation int64
		reason               string
	}{{5, 7, "source_revision_changed"}, {4, 8, "maintenance_generation_changed"}} {
		t.Run(tc.reason, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
			expectSourcePageFence(mock, tc.revision, tc.generation, true)
			mock.ExpectRollback()
			cursor, err := encodeSourcePageCursor(sourcePageCursor{Version: 1, Org: "org", Space: "space", Trace: "abc", Revision: 4, Generation: 7, Span: "001", Offset: 100, Count: 1, Bytes: 1000, ReadBytes: 100})
			require.NoError(t, err)
			result, err := s.traceSourcePage(context.Background(), "org", "space", "abc", cursor)
			require.NoError(t, err)
			require.Equal(t, readModelUnsupported(tc.reason), result)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSourcePageRejectsOversizeTraceBeforeFetchingFragments(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	expectSourcePageFence(mock, 4, 7, true)
	expectSourceRawSize(mock)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\),COALESCE\\(SUM\\(OCTET_LENGTH").WillReturnRows(sqlmock.NewRows([]string{"count", "bytes"}).AddRow(1, (128<<20)+1))
	mock.ExpectRollback()
	result, err := s.traceSourcePage(context.Background(), "org", "space", "abc", "")
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("source_trace_payload_limit"), result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSourcePageReconstructsEveryByteOfLargeUnicodeSpan(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	// The last field and exact int64 attribute catch truncation and float64
	// decoding. Repeated Unicode deliberately crosses byte fragment boundaries.
	wire, err := json.Marshal(map[string]any{"trace_id": "abc", "span_id": "001", "attributes": `{"exact":9007199254740993}`, "payload": `{"content":"` + strings.Repeat("观测🙂", 1900000) + `","tail":"retained"}`})
	require.NoError(t, err)
	require.Greater(t, len(wire), 16<<20)
	var reconstructed []byte
	cursor := ""
	for offset := 0; offset < len(wire); {
		size := 1280 << 10
		if size > len(wire)-offset {
			size = len(wire) - offset
		}
		expectSourcePageFence(mock, 4, 7, true)
		expectSourceRawSize(mock)
		span := "001"
		if offset == 0 {
			span = ""
			mock.ExpectQuery("SELECT COUNT\\(\\*\\),COALESCE\\(SUM\\(OCTET_LENGTH").WillReturnRows(sqlmock.NewRows([]string{"count", "bytes"}).AddRow(1, len(wire)))
		}
		mock.ExpectQuery("SELECT span_id,OCTET_LENGTH").WithArgs("org", "org", "space", "space", "abc", "abc", span, 129).WillReturnRows(sqlmock.NewRows([]string{"span_id", "total_bytes"}).AddRow("001", len(wire)))
		mock.ExpectQuery("SELECT 0 AS ordinal,span_id,SUBSTRING").WithArgs(int64(offset+1), int64(size), "org", "org", "space", "space", "abc", "abc", "001", "001").WillReturnRows(sqlmock.NewRows([]string{"ordinal", "span_id", "fragment"}).AddRow(0, "001", wire[offset:offset+size]))
		mock.ExpectCommit()
		value, err := s.traceSourcePage(context.Background(), "org", "space", "abc", cursor)
		require.NoError(t, err)
		page := value.(map[string]any)
		fragments := page["fragments"].([]sourcePageFragment)
		require.Len(t, fragments, 1)
		require.EqualValues(t, offset, fragments[0].Offset)
		chunk, err := base64.StdEncoding.DecodeString(fragments[0].DataBase64)
		require.NoError(t, err)
		reconstructed = append(reconstructed, chunk...)
		offset += size
		require.Equal(t, offset == len(wire), page["complete"])
		cursor = page["next_cursor"].(string)
		encoded, err := json.Marshal(page)
		require.NoError(t, err)
		require.LessOrEqual(t, len(encoded), 2<<20)
	}
	require.Empty(t, cursor)
	require.Equal(t, wire, reconstructed)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSourcePageSharesQueryAdmission(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	cfg := config{Enabled: true, QueryTimeout: time.Second, TablePrefix: "test"}
	h := &handler{backend: &oceanBaseStore{db: db, cfg: cfg}, cfg: cfg, logger: slog.Default()}
	require.True(t, acquireTraceQuery(httptest.NewRecorder()))
	require.True(t, acquireTraceQuery(httptest.NewRecorder()))
	defer func() { releaseTraceQuery(); releaseTraceQuery() }()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/ai-vision/traces/read-model/source-page?space_id=space&trace_id=abc", nil)
	r = r.WithContext(authtypes.NewContextWithClaims(r.Context(), authtypes.Claims{OrgID: supplementTestOrg}))
	w := httptest.NewRecorder()
	h.ReadModel(w, r)
	require.Equal(t, http.StatusTooManyRequests, w.Code)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSourcePageRawSizeGuardPrecedesJSONConstruction(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	expectSourcePageFence(mock, 4, 7, true)
	mock.ExpectQuery("SELECT COUNT.*SUM.*OCTET_LENGTH.*MAX.*OCTET_LENGTH").WillReturnRows(sqlmock.NewRows([]string{"count", "bytes", "max"}).AddRow(1, 129<<20, 129<<20))
	mock.ExpectRollback()
	result, err := s.traceSourcePage(context.Background(), "org", "space", "abc", "")
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("source_trace_payload_limit"), result)
	require.NoError(t, mock.ExpectationsWereMet())
}
