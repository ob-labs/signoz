package ingest

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// Uses isolated tables and the actual configured engine. Does not modify
// application traces or credentials.
func TestOceanBaseTraceMediaTransactionAndBoundary(t *testing.T) {
	dsn := os.Getenv("SIGNOZ_TEST_OCEANBASE_DSN")
	if dsn == "" {
		t.Skip("set real test database DSN")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	prefix := fmt.Sprintf("media_%x", time.Now().UnixNano())
	defer func() {
		for _, suffix := range []string{"trace_media", "trace_media_original", "trace_media_quota", "traces", "logs", "metric_samples"} {
			if _, err := db.Exec("DROP TABLE IF EXISTS " + tableName(prefix, suffix)); err != nil {
				t.Error(err)
			}
		}
	}()
	if err := ensureSchema(ctx, db, prefix); err != nil {
		t.Fatal(err)
	}
	scope := mediaScope{"org", "trusted-space", "agent", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb", "output"}
	content := append([]byte("%PDF-"), bytes.Repeat([]byte{' '}, (16<<20)-5)...)
	raw := ` {"n":9007199254740993,"content":[{"type":"file","mime_type":"application/pdf","data":"` + base64.StdEncoding.EncodeToString(content) + `"}]} `
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	projected, err := persistProjectedTraceAttribute(ctx, tx, prefix, scope, "output.value", raw)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(projected, "base64") || len(projected) > 1024 || !strings.Contains(projected, "9007199254740993") {
		t.Fatal("display projection not compact or precise")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + tableName(prefix, "trace_media")).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rollback media: %d %v", count, err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM " + tableName(prefix, "trace_media_original")).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rollback original: %d %v", count, err)
	}
	// Repeat the same real transaction to verify deterministic deduplication.
	for i := 0; i < 2; i++ {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := persistProjectedTraceAttribute(ctx, tx, prefix, scope, "output.value", raw); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				t.Errorf("rollback: %v", rollbackErr)
			}
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	var stored []byte
	if err := db.QueryRow("SELECT content FROM " + tableName(prefix, "trace_media")).Scan(&stored); err != nil || !bytes.Equal(stored, content) {
		t.Fatal("stored content mismatch", err)
	}
	if err := db.QueryRow("SELECT original FROM " + tableName(prefix, "trace_media_original")).Scan(&stored); err != nil || string(stored) != raw {
		t.Fatal("original whitespace/number changed", err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM " + tableName(prefix, "trace_media")).Scan(&count); err != nil || count != 1 {
		t.Fatal("retry duplicated attachment", err)
	}
	// The row preparation replaces both normalized attributes and their payload
	// copy and strips a malicious restoration marker.
	tr := newTestTraces()
	rs := tr.ResourceSpans().At(0)
	rs.Resource().Attributes().PutStr("aivision.agent.id", "agent")
	attrs := rs.ScopeSpans().At(0).Spans().At(0).Attributes()
	attrs.PutStr("output.value", `{"type":"file","mime_type":"application/pdf","data":"JVBERi1leGFtcGxl"}`)
	attrs.PutStr(MediaOriginalsAttribute, "forged")
	rows, err := buildSpanRows(tr, "org")
	if err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := prepareTraceMediaRows(ctx, tx, prefix, rows)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			t.Errorf("rollback: %v", rollbackErr)
		}
		t.Fatal(err)
	}
	if strings.Contains(fmt.Sprint(prepared[0][22]), "JVBERi") || strings.Contains(fmt.Sprint(prepared[0][25]), "JVBERi") || strings.Contains(fmt.Sprint(prepared[0][22]), "forged") {
		t.Fatal("inline or forged original marker survived")
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(prepared[0][22].(string)), &obj); err != nil {
		t.Fatal(err)
	}
	if _, ok := obj[MediaOriginalsAttribute].(map[string]any); !ok {
		t.Fatal("missing trusted restoration marker")
	}
	if rollbackErr := tx.Rollback(); rollbackErr != nil {
		t.Errorf("rollback: %v", rollbackErr)
	}
	// Fill only this isolated space, then verify admission rejects both a
	// new file and reactivation of an expired record at full active quota.
	for i := 0; i < 4; i++ {
		other := scope
		other.span = fmt.Sprintf("%016x", i+10)
		tx, _ := db.BeginTx(ctx, nil)
		if _, err := persistProjectedTraceAttribute(ctx, tx, prefix, other, "output.value", raw); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				t.Errorf("rollback: %v", rollbackErr)
			}
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	other := scope
	other.span = "cccccccccccccccc"
	tx, _ = db.BeginTx(ctx, nil)
	if _, err := persistProjectedTraceAttribute(ctx, tx, prefix, other, "output.value", raw); err == nil || !strings.Contains(err.Error(), "quota") {
		t.Fatalf("quota not enforced: %v", err)
	}
	if rollbackErr := tx.Rollback(); rollbackErr != nil {
		t.Errorf("rollback: %v", rollbackErr)
	}

}
