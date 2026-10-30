package aivision

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestDirtySupplementDeclinesMissingStoredVersionBeforeRawScan(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	store := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION").WithArgs("test_trace_projections", "metrics_version_v1").WillReturnRows(sqlmock.NewRows([]string{"kind", "extra", "expression"}))
	mock.ExpectRollback()
	result, err := store.dirtyTraceSources(context.Background(), "org", "space", "context", "", 20, traceMetricsSupplementVersion)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("projection_metrics_version_unavailable"), result)
	require.NoError(t, mock.ExpectationsWereMet())
}
