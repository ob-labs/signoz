package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/SigNoz/signoz/pkg/errors"
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
	defer func(tx *sql.Tx) { _ = tx.Rollback() }(tx)
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
		defer func() { _ = tx.Rollback() }()
		if _, err := persistProjectedTraceAttribute(ctx, tx, prefix, scope, "output.value", raw); err != nil {
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
	defer func(tx *sql.Tx) { _ = tx.Rollback() }(tx)
	prepared, err := prepareTraceMediaRows(ctx, tx, prefix, rows)
	if err != nil {
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
	store := func(span string, data []byte) (map[string]any, error) {
		return StoreTraceMedia(ctx, db, prefix, scope.org, scope.space, scope.agent, scope.trace, span, scope.direction, "application/pdf", data)
	}
	isQuota := func(err error) bool {
		return errors.Is(err, errTraceMediaSpaceQuota)
	}
	// The original 16 MiB plus five distinct spans is 96 MiB, not 112 MiB.
	for i := 0; i < 5; i++ {
		if _, err := store(fmt.Sprintf("%016x", i+10), content); err != nil {
			t.Fatalf("admission through 96 MiB: %v", err)
		}
	}
	if _, err := store("cccccccccccccccc", content); !isQuota(err) {
		t.Fatalf("112 MiB must be rejected: %v", err)
	}

	tinyPDF := []byte("%PDF-x")
	expiredSpan := "dddddddddddddddd"
	expired, err := store(expiredSpan, tinyPDF)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE "+tableName(prefix, "trace_media")+" SET created_at=? WHERE id=?", time.Now().Add(-31*24*time.Hour), expired["id"]); err != nil {
		t.Fatal(err)
	}

	// Two writers contend for the remaining 4 MiB. Exactly one may fill it.
	results := make(chan error, 2)
	for _, span := range []string{"eeeeeeeeeeeeeeee", "ffffffffffffffff"} {
		go func() {
			_, err := store(span, content[:4<<20])
			results <- err
		}()
	}
	first, second := <-results, <-results
	if (first != nil || !isQuota(second)) && (second != nil || !isQuota(first)) {
		t.Fatalf("exactly one writer must reach 100 MiB: %v, %v", first, second)
	}
	var used int64
	if err := db.QueryRowContext(ctx, "SELECT COALESCE(SUM(OCTET_LENGTH(content)),0) FROM "+tableName(prefix, "trace_media")+" WHERE created_at>=?", time.Now().Add(-30*24*time.Hour)).Scan(&used); err != nil || used != 100<<20 {
		t.Fatalf("active bytes: %d, error: %v", used, err)
	}
	if _, err := store(scope.span, content); err != nil {
		t.Fatalf("retry of active attachment at full quota: %v", err)
	}
	for _, span := range []string{"1111111111111111", expiredSpan} {
		if _, err := store(span, tinyPDF); !isQuota(err) {
			t.Fatalf("new or expired attachment %s at full quota: %v", span, err)
		}
	}

	// Inline OTLP at full quota still commits its span and captions. Both
	// directions receive budget descriptors while exact sources remain scoped
	// to the trusted restoration pointers, outside both display copies.
	quotaTraces := newTestTraces()
	quotaRS := quotaTraces.ResourceSpans().At(0)
	quotaRS.Resource().Attributes().PutStr("aivision.agent.id", scope.agent)
	quotaSpan := quotaRS.ScopeSpans().At(0).Spans().At(0)
	quotaSpan.SetName("quota-preserves-span")
	originals := map[string]string{}
	for _, tc := range []struct{ key, role, caption string }{
		{"input.value", "user", "input caption survives quota"},
		{"output.value", "assistant", "output caption survives quota"},
	} {
		raw := `[{"role":"` + tc.role + `","content":[{"type":"text","text":"` + tc.caption + `"},{"type":"file","mime_type":"application/pdf","data":"JVBERi1leGFtcGxl"}]}]`
		originals[tc.key] = raw
		quotaSpan.Attributes().PutStr(tc.key, raw)
	}
	quotaRows, err := buildSpanRows(quotaTraces, scope.org)
	if err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func(tx *sql.Tx) { _ = tx.Rollback() }(tx)
	prepared, err = prepareTraceMediaRows(ctx, tx, prefix, quotaRows)
	if err != nil {
		t.Fatalf("quota must not reject the OTLP span: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+spansTable(prefix)+spanInsertColumns+spanUpsert, prepared[0]...); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var name, attrsJSON, payloadJSON string
	if err := db.QueryRowContext(ctx, "SELECT span_name,attributes,payload FROM "+spansTable(prefix)+" WHERE org_id=? AND space_id=? AND trace_id=? AND span_id=?", quotaRows[0][0], quotaRows[0][1], quotaRows[0][3], quotaRows[0][4]).Scan(&name, &attrsJSON, &payloadJSON); err != nil {
		t.Fatal(err)
	}
	if name != "quota-preserves-span" || strings.Contains(attrsJSON, "JVBERi") || strings.Contains(payloadJSON, "JVBERi") {
		t.Fatal("span or compact display copies changed")
	}
	var quotaAttrs map[string]any
	if err := json.Unmarshal([]byte(attrsJSON), &quotaAttrs); err != nil {
		t.Fatal(err)
	}
	var quotaPayload struct {
		Attributes map[string]any `json:"attributes"`
	}
	if err := json.Unmarshal([]byte(payloadJSON), &quotaPayload); err != nil {
		t.Fatal(err)
	}
	pointers, ok := quotaAttrs[MediaOriginalsAttribute].(map[string]any)
	if !ok || len(pointers) != 2 {
		t.Fatal("missing exact-source restoration pointers")
	}
	for key, original := range originals {
		projected, ok := quotaAttrs[key].(string)
		if !ok || quotaPayload.Attributes[key] != projected {
			t.Fatalf("display copies disagree for %s", key)
		}
		var messages []struct {
			Role    string           `json:"role"`
			Content []map[string]any `json:"content"`
		}
		if err := json.Unmarshal([]byte(projected), &messages); err != nil {
			t.Fatal(err)
		}
		role, caption := "user", "input caption survives quota"
		if key == "output.value" {
			role, caption = "assistant", "output caption survives quota"
		}
		if len(messages) != 1 || messages[0].Role != role || len(messages[0].Content) != 2 || messages[0].Content[0]["text"] != caption || messages[0].Content[1]["type"] != "aivision_media_error" || messages[0].Content[1]["reason"] != "budget" {
			t.Fatalf("quota lost role/caption/order for %s: %s", key, projected)
		}
		projectionHash := sha256.Sum256([]byte(projected))
		if err := db.QueryRowContext(ctx, "SELECT original FROM "+tableName(prefix, "trace_media_original")+" WHERE id=? AND org_id=? AND space_id=? AND agent_id=? AND trace_id=? AND span_id=? AND attribute_key=? AND projected_sha256=?", pointers[key], quotaRows[0][0], quotaRows[0][1], scope.agent, quotaRows[0][3], quotaRows[0][4], key, hex.EncodeToString(projectionHash[:])).Scan(&stored); err != nil || string(stored) != original {
			t.Fatalf("quota source reconstruction for %s: %v", key, err)
		}
	}
	if err := db.QueryRowContext(ctx, "SELECT COALESCE(SUM(OCTET_LENGTH(content)),0) FROM "+tableName(prefix, "trace_media")+" WHERE created_at>=?", time.Now().Add(-30*24*time.Hour)).Scan(&used); err != nil || used != 100<<20 {
		t.Fatalf("inline quota relaxed admission: active bytes=%d error=%v", used, err)
	}

}
