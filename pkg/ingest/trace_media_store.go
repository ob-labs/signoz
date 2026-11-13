package ingest

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"github.com/SigNoz/signoz/pkg/errors"
	"regexp"
	"time"
)

// mediaScope comes from the authenticated OTLP resource and span, not from a
// model message. The caller must use the same transaction as the span write.
type mediaScope struct {
	org, space, agent, trace, span, direction string
}

var mediaTraceID = regexp.MustCompile(`^[0-9a-f]{32}$`)
var mediaSpanID = regexp.MustCompile(`^[0-9a-f]{16}$`)

// persistTraceMedia writes a content-addressed attachment in the caller's
// transaction. A reference is returned only after the database accepts it.
func persistTraceMedia(ctx context.Context, tx *sql.Tx, prefix string, scope mediaScope, media traceMedia) (string, error) {
	if !tablePrefixPattern.MatchString(prefix) || scope.org == "" || scope.space == "" || scope.agent == "" || !mediaTraceID.MatchString(scope.trace) || !mediaSpanID.MatchString(scope.span) || (scope.direction != "input" && scope.direction != "output") {
		return "", errors.NewInvalidInputf(errors.CodeInvalidInput, "invalid trace media scope")
	}
	if len(media.Data) == 0 || len(media.Data) > 16<<20 || !validTraceMedia(media.MIME, media.Data) {
		return "", errors.NewInvalidInputf(errors.CodeInvalidInput, "invalid trace media content")
	}
	hash := sha256.Sum256(media.Data)
	if media.SHA256 != hex.EncodeToString(hash[:]) {
		return "", errors.NewInvalidInputf(errors.CodeInvalidInput, "trace media digest mismatch")
	}
	// The scope is part of the identifier so identical bytes in two spaces or
	// spans never become a cross-tenant capability.
	id := digest(scope.org, scope.space, scope.agent, scope.trace, scope.span, scope.direction, media.MIME, media.SHA256)
	if err := lockTraceMediaQuota(ctx, tx, prefix, scope); err != nil {
		return "", err
	}
	var existing int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tableName(prefix, "trace_media")+" WHERE id=? AND created_at>=?", id, time.Now().Add(-30*24*time.Hour)).Scan(&existing); err != nil {
		return "", err
	}
	if existing == 0 {
		var used int64
		if err := tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(OCTET_LENGTH(content)),0) FROM "+tableName(prefix, "trace_media")+" WHERE org_id=? AND space_id=? AND created_at>=?", scope.org, scope.space, time.Now().Add(-30*24*time.Hour)).Scan(&used); err != nil {
			return "", err
		}
		if used+int64(len(media.Data)) > 100<<20 {
			return "", errors.NewInvalidInputf(errors.CodeInvalidInput, "trace media space quota exceeded")
		}
	}
	query := "INSERT INTO " + tableName(prefix, "trace_media") + " (id,org_id,space_id,agent_id,trace_id,span_id,direction,mime_type,sha256,content) VALUES (?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE created_at=CURRENT_TIMESTAMP(6)"
	if _, err := tx.ExecContext(ctx, query, id, scope.org, scope.space, scope.agent, scope.trace, scope.span, scope.direction, media.MIME, media.SHA256, media.Data); err != nil {
		return "", errors.WrapInternalf(err, errors.CodeInternal, "store trace media")
	}
	return id, nil
}

// persistTraceMediaOriginal retains the exact original UTF-8 attribute. It is
// keyed by the projection digest as a span can be re-sent with changed input.
func persistTraceMediaOriginal(ctx context.Context, tx *sql.Tx, prefix string, scope mediaScope, attribute, projected, original string) error {
	if !tablePrefixPattern.MatchString(prefix) || scope.org == "" || scope.space == "" || scope.agent == "" || !mediaTraceID.MatchString(scope.trace) || !mediaSpanID.MatchString(scope.span) || attribute == "" || len(attribute) > 255 || projected == "" || original == "" {
		return errors.NewInvalidInputf(errors.CodeInvalidInput, "invalid trace media original")
	}
	hash := sha256.Sum256([]byte(projected))
	projectionDigest := hex.EncodeToString(hash[:])
	id := traceMediaOriginalID(scope, attribute, projected, original)
	query := "INSERT INTO " + tableName(prefix, "trace_media_original") + " (id,org_id,space_id,agent_id,trace_id,span_id,attribute_key,projected_sha256,original) VALUES (?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE id=VALUES(id)"
	if _, err := tx.ExecContext(ctx, query, id, scope.org, scope.space, scope.agent, scope.trace, scope.span, attribute, projectionDigest, []byte(original)); err != nil {
		return errors.WrapInternalf(err, errors.CodeInternal, "store trace media original")
	}
	return nil
}

// persistProjectedTraceAttribute must be called inside the span transaction.
// It returns the original untouched value when no supported media is found.
// Callers must roll back if it returns an error.
func persistProjectedTraceAttribute(ctx context.Context, tx *sql.Tx, prefix string, scope mediaScope, attribute, original string) (string, error) {
	projected, err := projectTraceMedia(original, 16<<20, func(media traceMedia) (string, error) {
		return persistTraceMedia(ctx, tx, prefix, scope, media)
	}, scope)
	if err != nil {
		return "", err
	}
	if projected == original {
		return original, nil
	}
	if err := persistTraceMediaOriginal(ctx, tx, prefix, scope, attribute, projected, original); err != nil {
		return "", err
	}
	return projected, nil
}

func traceMediaOriginalID(scope mediaScope, attribute, projected, original string) string {
	ph := sha256.Sum256([]byte(projected))
	oh := sha256.Sum256([]byte(original))
	return digest(scope.org, scope.space, scope.agent, scope.trace, scope.span, attribute, hex.EncodeToString(ph[:]), hex.EncodeToString(oh[:]))
}

// StoreTraceMedia is the data-plane upload service; HTTP callers never access
// the database or construct storage identities themselves.
func StoreTraceMedia(ctx context.Context, db *sql.DB, prefix, org, space, agent, trace, span, direction, mime string, content []byte) (map[string]any, error) {
	hash := sha256.Sum256(content)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	scope := mediaScope{org, space, agent, trace, span, direction}
	id, err := persistTraceMedia(ctx, tx, prefix, scope, traceMedia{mime, hex.EncodeToString(hash[:]), content})
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return map[string]any{"id": id, "type": "aivision_attachment", "mimeType": mime, "sha256": hex.EncodeToString(hash[:]), "spaceId": space, "agentId": agent}, nil
}

func lockTraceMediaQuota(ctx context.Context, tx *sql.Tx, prefix string, scope mediaScope) error {
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+tableName(prefix, "trace_media_quota")+" (org_id,space_id) VALUES (?,?) ON DUPLICATE KEY UPDATE space_id=VALUES(space_id)", scope.org, scope.space); err != nil {
		return err
	}
	var space string
	return tx.QueryRowContext(ctx, "SELECT space_id FROM "+tableName(prefix, "trace_media_quota")+" WHERE org_id=? AND space_id=? FOR UPDATE", scope.org, scope.space).Scan(&space)
}
