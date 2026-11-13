package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"github.com/SigNoz/signoz/pkg/errors"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPersistTraceMediaUsesCallerTransactionAndPropagatesFailure(t *testing.T) {
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
	data := []byte("%PDF-example")
	hash := sha256.Sum256(data)
	media := traceMedia{MIME: "application/pdf", SHA256: hex.EncodeToString(hash[:]), Data: data}
	scope := mediaScope{"org", "space", "agent", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb", "output"}
	query := "INSERT INTO `test_trace_media` (id,org_id,space_id,agent_id,trace_id,span_id,direction,mime_type,sha256,content) VALUES (?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE created_at=CURRENT_TIMESTAMP(6)"
	expectMediaQuota(mock)
	mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs(sqlmock.AnyArg(), "org", "space", "agent", scope.trace, scope.span, "output", media.MIME, media.SHA256, data).WillReturnError(errors.NewInternalf(errors.CodeInternal, "storage failure"))
	mock.ExpectRollback()
	if id, err := persistTraceMedia(context.Background(), tx, "test", scope, media); err == nil || id != "" {
		t.Fatal("failed write must not return a reference")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPersistProjectedAttributeDoesNotReturnUnrecoverableReference(t *testing.T) {
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
	scope := mediaScope{"org", "space", "agent", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb", "output"}
	raw := `{"type":"file","mime_type":"application/pdf","data":"` + base64.StdEncoding.EncodeToString([]byte("%PDF-example")) + `"}`
	expectMediaQuota(mock)
	mock.ExpectExec("INSERT INTO `test_trace_media`").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO `test_trace_media_original`").WillReturnError(errors.NewInternalf(errors.CodeInternal, "original failed"))
	mock.ExpectRollback()
	if value, err := persistProjectedTraceAttribute(context.Background(), tx, "test", scope, "output.value", raw); err == nil || value != "" {
		t.Fatal("original failure must not return a reference")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPersistProjectedAttributeStoresMediaAndOriginal(t *testing.T) {
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
	scope := mediaScope{"org", "space", "agent", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb", "output"}
	raw := `{"type":"file","mime_type":"application/pdf","data":"` + base64.StdEncoding.EncodeToString([]byte("%PDF-example")) + `"}`
	expectMediaQuota(mock)
	mock.ExpectExec("INSERT INTO `test_trace_media`").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO `test_trace_media_original`").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	value, err := persistProjectedTraceAttribute(context.Background(), tx, "test", scope, "output.value", raw)
	if err != nil || !strings.Contains(value, `"type":"aivision_attachment"`) {
		t.Fatalf("projection %q: %v", value, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOriginalIdentityIncludesExactSourceBytes(t *testing.T) {
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
	scope := mediaScope{"org", "space", "agent", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb", "output"}
	projected := `{"type":"aivision_attachment","id":"x"}`
	projectedHash := sha256.Sum256([]byte(projected))
	projectionDigest := hex.EncodeToString(projectedHash[:])
	for _, raw := range []string{`{"value":1}`, `{ "value": 1 }`} {
		rawHash := sha256.Sum256([]byte(raw))
		id := digest(scope.org, scope.space, scope.agent, scope.trace, scope.span, "output.value", projectionDigest, hex.EncodeToString(rawHash[:]))
		mock.ExpectExec("INSERT INTO `test_trace_media_original`").WithArgs(id, scope.org, scope.space, scope.agent, scope.trace, scope.span, "output.value", projectionDigest, []byte(raw)).WillReturnResult(sqlmock.NewResult(0, 1))
		if err := persistTraceMediaOriginal(context.Background(), tx, "test", scope, "output.value", projected, raw); err != nil {
			t.Fatal(err)
		}
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPersistTraceMediaRejectsUntrustedScopeAndDigest(t *testing.T) {
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
	data := []byte("%PDF-example")
	hash := sha256.Sum256(data)
	media := traceMedia{MIME: "application/pdf", SHA256: hex.EncodeToString(hash[:]), Data: data}
	valid := mediaScope{"org", "space", "agent", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb", "input"}
	for _, scope := range []mediaScope{
		{"org", "", "agent", valid.trace, valid.span, "input"},
		{"org", "space", "", valid.trace, valid.span, "input"},
		{"org", "space", "agent", "invalid", valid.span, "input"},
		{"org", "space", "agent", valid.trace, valid.span, "other"},
	} {
		if _, err := persistTraceMedia(context.Background(), tx, "test", scope, media); err == nil {
			t.Fatal("invalid scope accepted")
		}
	}
	media.SHA256 = "incorrect"
	if _, err := persistTraceMedia(context.Background(), tx, "test", valid, media); err == nil {
		t.Fatal("incorrect digest accepted")
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOriginalAndAttachmentRollbackTogether(t *testing.T) {
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
	data := []byte("%PDF-example")
	hash := sha256.Sum256(data)
	media := traceMedia{MIME: "application/pdf", SHA256: hex.EncodeToString(hash[:]), Data: data}
	scope := mediaScope{"org", "space", "agent", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb", "output"}
	expectMediaQuota(mock)
	mock.ExpectExec("INSERT INTO `test_trace_media`").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO `test_trace_media_original`").WillReturnError(errors.NewInternalf(errors.CodeInternal, "original failed"))
	mock.ExpectRollback()
	if _, err := persistTraceMedia(context.Background(), tx, "test", scope, media); err != nil {
		t.Fatal(err)
	}
	if err := persistTraceMediaOriginal(context.Background(), tx, "test", scope, "output.value", "projected", "original"); err == nil {
		t.Fatal("expected failure")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func expectMediaQuota(mock sqlmock.Sqlmock) {
	mock.ExpectExec("INSERT INTO `test_trace_media_quota`").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT space_id FROM `test_trace_media_quota`").WillReturnRows(sqlmock.NewRows([]string{"space_id"}).AddRow("space"))
	mock.ExpectQuery("SELECT COUNT.*FROM `test_trace_media`").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery("SELECT COALESCE.*FROM `test_trace_media`").WillReturnRows(sqlmock.NewRows([]string{"used"}).AddRow(0))
}
