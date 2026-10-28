package aivision

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func metadataCoverageMatcher(expected, actual string) error {
	if expected == "coverage_without_lob" {
		if !strings.HasPrefix(actual, "SELECT COALESCE") || strings.Contains(actual, "JSON_EXTRACT") || strings.Contains(actual, "_contentComplete") {
			return fmt.Errorf("metadata coverage must not request a JSON LOB")
		}
		return nil
	}
	matched, err := regexp.MatchString(expected, actual)
	if err != nil {
		return err
	}
	if !matched {
		return fmt.Errorf("query did not match %s", expected)
	}
	return nil
}

func TestMetadataTraceCoverageDoesNotRequestJSONLOB(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(metadataCoverageMatcher)))
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT complete FROM").WillReturnRows(sqlmock.NewRows([]string{"complete"}).AddRow(true))
	mock.ExpectQuery("coverage_without_lob").WillReturnRows(sqlmock.NewRows([]string{"stale", "boundary", "incomplete_content"}).AddRow(0, 0, 0))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery("SELECT JSON_OBJECT").WillReturnRows(sqlmock.NewRows([]string{"row_json", "preview_json", "source_revision"}))
	for range 4 {
		mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"value", "count"}))
	}
	mock.ExpectCommit()
	p := sessionReadModelTestParams().nativeListParams
	p.MetadataOnly = true
	result, err := s.readModelPage(context.Background(), "org", p)
	require.NoError(t, err)
	require.Equal(t, true, result.(map[string]any)["supported"])
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMetadataSessionCoverageDoesNotRequestJSONLOB(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(metadataCoverageMatcher)))
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT complete FROM").WillReturnRows(sqlmock.NewRows([]string{"complete"}).AddRow(true))
	mock.ExpectQuery("coverage_without_lob").WillReturnRows(sqlmock.NewRows([]string{"stale", "boundary", "incomplete_content"}).AddRow(0, 0, 0))
	mock.ExpectRollback()
	tx, err := db.Begin()
	require.NoError(t, err)
	reason, err := s.sessionReadModelCoverage(context.Background(), tx, "org", sessionReadModelTestParams())
	require.NoError(t, err)
	require.Empty(t, reason)
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

// An incomplete root IO document must decline before content predicates can
// turn its absent tail into a successful empty page.
func TestReadModelPageDeclinesIncompleteContentBeforeFilteredCounts(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT complete FROM").WillReturnRows(sqlmock.NewRows([]string{"complete"}).AddRow(true))
	expectContentCapability(mock, false)
	mock.ExpectQuery("SELECT COALESCE").WillReturnRows(sqlmock.NewRows([]string{"stale", "boundary", "incomplete_content"}).AddRow(0, 0, 1))
	mock.ExpectRollback()
	p := sessionReadModelTestParams().nativeListParams
	p.Keyword, p.SearchType = "tail-only", "input"
	result, err := s.readModelPage(context.Background(), "org", p)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("content_projection_incomplete"), result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProjectionRejectsUnboundedOperationPreviewFields(t *testing.T) {
	p := validProjection()
	var row map[string]any
	require.NoError(t, json.Unmarshal(p.Row, &row))
	row["_operation_previews"] = map[string]any{"span": map[string]any{"name": "LLM", "latency": 1, "status": 1, "input": nil, "output": nil, "input_truncated": false, "output_truncated": false, "raw_payload": "not part of the bounded preview contract"}}
	p.Row, _ = json.Marshal(row)
	require.Error(t, validateTraceProjection(p))
}

// Native span_name is VARCHAR(512), whose utf8mb4 capacity is characters.
// A valid long child name must not block publication of its small root summary.
func TestProjectionAcceptsNativeUnicodeChildOperationName(t *testing.T) {
	p := validProjection()
	p.Name = "root"
	var row map[string]any
	require.NoError(t, json.Unmarshal(p.Row, &row))
	row["_operation_previews"] = map[string]any{"child": map[string]any{"name": strings.Repeat("观", 512), "latency": 1, "status": 1, "input": nil, "output": nil, "input_truncated": false, "output_truncated": false}}
	p.Row, _ = json.Marshal(row)
	require.NoError(t, validateTraceProjection(p))
}

func TestSessionPageDeclinesIncompleteContentBeforeGroupSelection(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	s := &oceanBaseStore{db: db, cfg: config{TablePrefix: "test"}}
	mock.ExpectBegin()
	expectProjectionMaintenance(mock, true, false)
	mock.ExpectQuery("SELECT complete FROM").WillReturnRows(sqlmock.NewRows([]string{"complete"}).AddRow(true))
	expectContentCapability(mock, false)
	mock.ExpectQuery("SELECT COALESCE").WillReturnRows(sqlmock.NewRows([]string{"stale", "boundary", "incomplete_content"}).AddRow(0, 0, 1))
	mock.ExpectRollback()
	p := sessionReadModelTestParams()
	p.Keyword, p.SearchType = "tail-only", "output"
	result, err := s.sessionReadModelPage(context.Background(), "org", p)
	require.NoError(t, err)
	require.Equal(t, readModelUnsupported("content_projection_incomplete"), result)
	require.NoError(t, mock.ExpectationsWereMet())
}
