package aivision

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func publicationItems(count int) []traceProjection {
	result := make([]traceProjection, 0, count)
	for i := 1; i <= count; i++ {
		p := validProjection()
		p.TraceID = fmt.Sprintf("%032x", i)
		p.UserID = "owner"
		p.AgentProduct = "product"
		p.Row = json.RawMessage(fmt.Sprintf(`{"trace_id":%q,"start_time_unix_nano":"100000000"}`, p.TraceID))
		result = append(result, p)
	}
	return result
}

func expectPublicationSource(mock sqlmock.Sqlmock, p traceProjection, revision int64) {
	mock.ExpectQuery("SELECT source_revision,user_id,agent_product,scope_valid").WithArgs("org", p.SpaceID, p.TraceID).WillReturnRows(sqlmock.NewRows([]string{"revision", "user", "product", "valid"}).AddRow(revision, "owner", "product", true))
}
func expectPublicationWrite(mock sqlmock.Sqlmock, p traceProjection, failure error) {
	write := mock.ExpectExec("INSERT INTO .*trace_projections")
	if failure != nil {
		write.WillReturnError(failure)
		return
	}
	write.WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE .*trace_sources.* SET dirty=FALSE").WithArgs("org", p.SpaceID, p.TraceID, p.SourceRevision).WillReturnResult(sqlmock.NewResult(0, 1))
}
func expectPublicationChunk(mock sqlmock.Sqlmock, items []traceProjection) {
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, true)
	for _, p := range items {
		expectPublicationSource(mock, p, p.SourceRevision)
		expectPublicationWrite(mock, p, nil)
	}
	mock.ExpectCommit()
}

func TestPublicationBatchCommitsAtMost25AndReturnsOrderedReceipts(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	items := publicationItems(26)
	expectPublicationChunk(mock, items[:25])
	expectPublicationChunk(mock, items[25:])
	value, err := s.publishTraceBatch(context.Background(), "org", items)
	require.NoError(t, err)
	data := value.(map[string]any)["data"].([]any)
	require.Len(t, data, 26)
	for i, p := range items {
		r := data[i].(map[string]any)
		require.Equal(t, p.TraceID, r["trace_id"])
		require.Equal(t, true, r["published"])
		require.EqualValues(t, 1, r["source_revision"])
		require.EqualValues(t, 1, r["maintenance_generation"])
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPublicationBatchObservesEmergencyOffBetweenChunks(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	items := publicationItems(26)
	expectPublicationChunk(mock, items[:25])
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, false, true)
	mock.ExpectRollback()
	value, err := s.publishTraceBatch(context.Background(), "org", items)
	require.NoError(t, err)
	data := value.(map[string]any)["data"].([]any)
	require.Len(t, data, 26)
	require.Equal(t, true, data[24].(map[string]any)["published"])
	require.Equal(t, map[string]any{"supported": false, "reason": "projection_maintenance_unavailable", "trace_id": items[25].TraceID}, data[25])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPublicationBatchFailureRollsBackCurrentChunkAndKeepsEarlierCommit(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	items := publicationItems(27)
	expectPublicationChunk(mock, items[:25])
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, true)
	expectPublicationSource(mock, items[25], 1)
	expectPublicationWrite(mock, items[25], nil)
	failure := errors.New("publication row write failed")
	expectPublicationSource(mock, items[26], 1)
	expectPublicationWrite(mock, items[26], failure)
	mock.ExpectRollback()
	value, err := s.publishTraceBatch(context.Background(), "org", items)
	require.ErrorIs(t, err, failure)
	require.Nil(t, value, "rolled-back chunk must not return tentative successes")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPublicationBatchKeepsMixedPerTraceCASAndScopeReceipts(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	items := publicationItems(5)
	items[1].MaintenanceGeneration = 2
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, true)
	expectPublicationSource(mock, items[0], 1)
	expectPublicationWrite(mock, items[0], nil)
	// Item1's old generation is rejected before source access; item2 is stale.
	expectPublicationSource(mock, items[2], 2)
	mock.ExpectQuery("SELECT source_revision,user_id,agent_product,scope_valid").WithArgs("org", items[3].SpaceID, items[3].TraceID).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT source_revision,user_id,agent_product,scope_valid").WithArgs("org", items[4].SpaceID, items[4].TraceID).WillReturnRows(sqlmock.NewRows([]string{"revision", "user", "product", "valid"}).AddRow(1, "other-owner", "product", true))
	mock.ExpectCommit()
	value, err := s.publishTraceBatch(context.Background(), "org", items)
	require.NoError(t, err)
	data := value.(map[string]any)["data"].([]any)
	require.Len(t, data, 5)
	require.Equal(t, true, data[0].(map[string]any)["published"])
	for i, reason := range []string{"maintenance_generation_changed", "source_revision_changed", "source_not_enrolled", "source_scope_unprojectable"} {
		require.Equal(t, map[string]any{"supported": false, "reason": reason, "trace_id": items[i+1].TraceID}, data[i+1])
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPublicationBatchCommitFailureNeverReturnsTentativeSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	items := publicationItems(2)
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, true)
	for _, p := range items {
		expectPublicationSource(mock, p, 1)
		expectPublicationWrite(mock, p, nil)
	}
	failure := errors.New("publication commit failed")
	mock.ExpectCommit().WillReturnError(failure)
	value, err := s.publishTraceBatch(context.Background(), "org", items)
	require.ErrorIs(t, err, failure)
	require.Nil(t, value)
	require.NoError(t, mock.ExpectationsWereMet())
}
