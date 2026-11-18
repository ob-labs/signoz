package aivision

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestMediaOriginalRestorationBindsEveryScopeAndProjection(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	projected := `{"type":"aivision_attachment","id":"` + strings.Repeat("a", 64) + `","mimeType":"application/pdf"}`
	original := ` {"n":9007199254740993,"data":"exact original"} `
	hash := sha256.Sum256([]byte(projected))
	id := strings.Repeat("b", 64)
	attrs := map[string]any{"output.value": projected, ingest.MediaOriginalsAttribute: map[string]any{"output.value": id}}
	span := map[string]any{"space_id": "space", "trace_id": "trace", "span_id": "span", "resource": map[string]any{"aivision.agent.id": "agent"}, "attributes": attrs, "payload": map[string]any{"attributes": attrs}}
	mock.ExpectQuery("SELECT original FROM `test_trace_media_original`").WithArgs(id, "org", "space", "agent", "trace", "span", "output.value", hex.EncodeToString(hash[:])).WillReturnRows(sqlmock.NewRows([]string{"original"}).AddRow([]byte(original)))
	require.NoError(t, store.restoreTraceMedia(context.Background(), "org", []map[string]any{span}))
	require.Equal(t, original, span["attributes"].(map[string]any)["output.value"])
	require.NotContains(t, span, "payload")
	require.NotContains(t, span["attributes"], ingest.MediaOriginalsAttribute)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMediaReferenceMustBelongToCurrentScopedSpan(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprint(stale), func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
			id := strings.Repeat("a", 64)
			mock.ExpectQuery("SELECT id,space_id,agent_id").WithArgs(id, "org", "space", sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"id", "space_id", "agent_id", "trace_id", "span_id", "direction", "mime_type", "size"}).AddRow(id, "space", "agent", "trace", "span", "output", "application/pdf", 100))
			attrs := `{"output.value":"{\"type\":\"aivision_attachment\",\"id\":\"` + id + `\",\"mimeType\":\"application/pdf\"}"}`
			if stale {
				attrs = `{"output.value":"replaced"}`
			}
			mock.ExpectQuery("SELECT attributes FROM `test_traces` WHERE org_id").WithArgs("org", sqlmock.AnyArg(), sqlmock.AnyArg(), "space", "trace", "span", "agent").WillReturnRows(sqlmock.NewRows([]string{"attributes"}).AddRow(attrs))
			_, err = store.traceMediaMetadata(context.Background(), "org", id, nativeListParams{SpaceID: "space", From: time.Now().Add(-time.Hour).UnixMilli(), To: time.Now().UnixMilli()})
			if stale {
				require.ErrorIs(t, err, errNotFound)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestMediaDisplayIdentityEnrichmentPreservesSerializedBoundaries(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	raw := `{"n":9007199254740993,"content":"{\"type\":\"aivision_attachment\",\"id\":\"media\",\"mimeType\":\"audio/wav\",\"sha256\":\"untrusted\"}"}`
	attrs := map[string]any{"output.value": raw, ingest.MediaOriginalsAttribute: map[string]any{"output.value": "original"}}
	span := map[string]any{"space_id": "space", "trace_id": "trace", "span_id": "span", "resource": map[string]any{"aivision.agent.id": "agent"}, "attributes": attrs}
	mock.ExpectQuery("SELECT id,sha256 FROM `test_trace_media`").WithArgs("org", "space", "agent", "trace").WillReturnRows(sqlmock.NewRows([]string{"id", "sha256"}).AddRow("media", strings.Repeat("a", 64)))
	require.NoError(t, store.enrichTraceMedia(context.Background(), "org", []map[string]any{span}))
	require.Contains(t, attrs["output.value"], `9007199254740993`)
	require.Contains(t, attrs["output.value"], strings.Repeat("a", 64))
	require.NotContains(t, attrs["output.value"], "untrusted")
	var outer map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(attrs["output.value"].(string)), &outer))
	var nested string
	require.NoError(t, json.Unmarshal(outer["content"], &nested))
	require.True(t, strings.HasPrefix(nested, "{"))
	require.Equal(t, map[string]any{"output.value": "original"}, attrs[ingest.MediaOriginalsAttribute])
	require.NoError(t, mock.ExpectationsWereMet())
}
