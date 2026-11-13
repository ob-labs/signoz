package aivision

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/gorilla/mux"
)

const traceMediaMaxBytes = 16 << 20

var traceMediaID = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Restoration follows server-created pointers and verifies both the complete
// identity and the current projected field. No reference from another span can
// cause content to be inserted into this response.
func (s *oceanBaseStore) restoreTraceMedia(ctx context.Context, org string, spans []map[string]any) error {
	for _, span := range spans {
		attrs, ok := span["attributes"].(map[string]any)
		if !ok {
			continue
		}
		pointers, ok := attrs[ingest.MediaOriginalsAttribute].(map[string]any)
		if !ok {
			continue
		}
		// Remove shadowed transport copies before restoring potentially large
		// originals. Exact source values remain under attributes.
		if err := compactNativeSpan(span); err != nil {
			return err
		}
		attrs, _ = span["attributes"].(map[string]any)
		resource, _ := span["resource"].(map[string]any)
		for key, value := range pointers {
			id, ok := value.(string)
			if !ok || !traceMediaID.MatchString(id) {
				return fmt.Errorf("invalid media original reference")
			}
			projected, ok := attrs[key].(string)
			if !ok {
				return fmt.Errorf("invalid projected media attribute")
			}
			hash := sha256.Sum256([]byte(projected))
			var original []byte
			err := s.db.QueryRowContext(ctx, "SELECT original FROM "+s.readModelTable("trace_media_original")+" WHERE id=? AND org_id=? AND space_id=? AND agent_id=? AND trace_id=? AND span_id=? AND attribute_key=? AND projected_sha256=?", id, org, span["space_id"], resource["aivision.agent.id"], span["trace_id"], span["span_id"], key, hex.EncodeToString(hash[:])).Scan(&original)
			if err != nil {
				return fmt.Errorf("media original unavailable: %w", err)
			}
			attrs[key] = string(original)
			if payload, ok := span["payload"].(map[string]any); ok {
				if pa, ok := payload["attributes"].(map[string]any); ok {
					pa[key] = string(original)
					delete(pa, ingest.MediaOriginalsAttribute)
				}
			}
		}
		delete(attrs, ingest.MediaOriginalsAttribute)
	}
	return nil
}

type traceMediaMetadata struct {
	ID        string `json:"id"`
	SpaceID   string `json:"spaceId"`
	AgentID   string `json:"agentId"`
	TraceID   string `json:"traceId"`
	SpanID    string `json:"spanId"`
	Direction string `json:"direction"`
	MIME      string `json:"mimeType"`
	Size      int    `json:"size"`
}

// A media record alone is insufficient: it must still be referenced by its
// current, authorized span. Superseded output is therefore not downloadable.
type mediaReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *oceanBaseStore) traceMediaMetadata(ctx context.Context, org, id string, p nativeListParams) (traceMediaMetadata, error) {
	return s.traceMediaMetadataFrom(ctx, s.db, org, id, p)
}
func (s *oceanBaseStore) traceMediaMetadataFrom(ctx context.Context, reader mediaReader, org, id string, p nativeListParams) (traceMediaMetadata, error) {
	var m traceMediaMetadata
	err := reader.QueryRowContext(ctx, "SELECT id,space_id,agent_id,trace_id,span_id,direction,mime_type,OCTET_LENGTH(content) FROM "+s.readModelTable("trace_media")+" WHERE id=? AND org_id=? AND space_id=? AND created_at>=?", id, org, p.SpaceID, time.Now().Add(-30*24*time.Hour)).Scan(&m.ID, &m.SpaceID, &m.AgentID, &m.TraceID, &m.SpanID, &m.Direction, &m.MIME, &m.Size)
	if err == sql.ErrNoRows {
		return m, errNotFound
	}
	if err != nil {
		return m, err
	}
	if p.AgentUUID != "" && p.AgentUUID != m.AgentID {
		return m, errNotFound
	}
	if m.Size <= 0 || m.Size > traceMediaMaxBytes {
		return m, errNotFound
	}
	// Attachment authorization is not limited to the UI's default 24-hour
	// trace window. Retained media remains readable for the full retention.
	p.From = time.Now().Add(-30 * 24 * time.Hour).UnixMilli()
	p.To = time.Now().Add(time.Millisecond).UnixMilli()
	p.TraceID = m.TraceID
	p.SpanIDs = []string{m.SpanID}
	where, args, err := nativeTraceWhere(org, p)
	if err != nil {
		return m, err
	}
	var attrsRaw string
	err = reader.QueryRowContext(ctx, "SELECT attributes FROM "+s.tracesTable()+" WHERE "+where+" AND "+traceResourceTextSQL("aivision.agent.id")+"=? LIMIT 1", append(args, m.AgentID)...).Scan(&attrsRaw)
	if err == sql.ErrNoRows {
		return m, errNotFound
	}
	if err != nil {
		return m, err
	}
	var attrs map[string]any
	dec := json.NewDecoder(strings.NewReader(attrsRaw))
	dec.UseNumber()
	if dec.Decode(&attrs) != nil {
		return m, errNotFound
	}
	for key, value := range attrs {
		direction := ""
		if strings.Contains(key, "input") || key == "gen_ai.tool.call.arguments" {
			direction = "input"
		}
		if strings.Contains(key, "output") || key == "gen_ai.tool.call.result" {
			direction = "output"
		}
		if direction == m.Direction && mediaReferencePresent(value, id, m.MIME, 0) {
			return m, nil
		}
	}
	return m, errNotFound
}

func mediaReferencePresent(value any, id, mime string, depth int) bool {
	if depth > 20 {
		return false
	}
	switch v := value.(type) {
	case string:
		if !strings.Contains(v, id) {
			return false
		}
		var parsed any
		d := json.NewDecoder(strings.NewReader(v))
		d.UseNumber()
		if d.Decode(&parsed) != nil {
			return false
		}
		return mediaReferencePresent(parsed, id, mime, depth+1)
	case map[string]any:
		if v["type"] == "aivision_attachment" && v["id"] == id && v["mimeType"] == mime {
			return true
		}
		for _, child := range v {
			if mediaReferencePresent(child, id, mime, depth+1) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if mediaReferencePresent(child, id, mime, depth+1) {
				return true
			}
		}
	}
	return false
}

func (h *handler) TraceMedia(w http.ResponseWriter, r *http.Request) {
	org, ok := h.organization(w, r)
	if !ok {
		return
	}
	if strings.HasSuffix(r.URL.Path, "/limits") {
		writeJSON(w, 200, map[string]any{"maxBytes": traceMediaMaxBytes})
		return
	}
	id := mux.Vars(r)["mediaID"]
	if !traceMediaID.MatchString(id) {
		http.NotFound(w, r)
		return
	}
	p, err := h.nativeParams(r)
	if err != nil {
		writeAPIError(w, 400, "bad_data", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.cfg.QueryTimeout)
	defer cancel()
	tx, err := h.backend.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		h.writeBackendError(w, err)
		return
	}
	defer tx.Rollback()
	m, err := h.backend.traceMediaMetadataFrom(ctx, tx, org, id, p)
	if err != nil {
		h.writeBackendError(w, err)
		return
	}
	if !strings.HasSuffix(r.URL.Path, "/content") {
		writeJSON(w, 200, m)
		return
	}
	var content []byte
	if err := tx.QueryRowContext(ctx, "SELECT content FROM "+h.backend.readModelTable("trace_media")+" WHERE id=? AND org_id=? AND space_id=? AND created_at>=?", id, org, p.SpaceID, time.Now().Add(-30*24*time.Hour)).Scan(&content); err != nil {
		h.writeBackendError(w, err)
		return
	}
	if err := tx.Commit(); err != nil {
		h.writeBackendError(w, err)
		return
	}
	w.Header().Set("Content-Type", m.MIME)
	w.Header().Set("Content-Disposition", "inline")
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; sandbox")
	w.WriteHeader(200)
	_, _ = w.Write(content)
}

// Content identity is display metadata only. Enrich older references without
// rewriting stored spans or the exact values used for original restoration.
func (s *oceanBaseStore) enrichTraceMedia(ctx context.Context, org string, spans []map[string]any) error {
	cache := map[string]map[string]string{}
	for _, span := range spans {
		attrs, ok := span["attributes"].(map[string]any)
		if !ok {
			continue
		}
		resource, _ := span["resource"].(map[string]any)
		var refs []map[string]any
		var hashes map[string]string
		var walk func(any, int) any
		walk = func(value any, depth int) any {
			if depth > 20 {
				return value
			}
			switch v := value.(type) {
			case string:
				if !strings.Contains(v, "aivision_attachment") {
					return v
				}
				var nested any
				dec := json.NewDecoder(strings.NewReader(v))
				dec.UseNumber()
				if dec.Decode(&nested) != nil {
					return v
				}
				encoded, err := json.Marshal(walk(nested, depth+1))
				if err != nil {
					return v
				}
				return string(encoded)
			case map[string]any:
				if v["type"] == "aivision_attachment" {
					refs = append(refs, v)
					if hashes != nil {
						delete(v, "sha256")
						if id, ok := v["id"].(string); ok && hashes[id] != "" {
							v["sha256"] = hashes[id]
						}
					}
					return v
				}
				for key, child := range v {
					v[key] = walk(child, depth+1)
				}
			case []any:
				for i, child := range v {
					v[i] = walk(child, depth+1)
				}
			}
			return value
		}
		// Parse only media-bearing fields, retaining their serialized boundary.
		parsed := map[string]any{}
		for key, value := range attrs {
			if raw, ok := value.(string); ok && strings.Contains(raw, "aivision_attachment") {
				var value any
				dec := json.NewDecoder(strings.NewReader(raw))
				dec.UseNumber()
				if dec.Decode(&value) == nil {
					parsed[key] = walk(value, 0)
				}
			}
		}
		if len(refs) == 0 {
			continue
		}
		// A trace may expose hundreds of media-bearing spans. Read lightweight
		// identities once per authorized trace, never binary data for the summary.
		cacheKey := fmt.Sprint(span["space_id"], "/", resource["aivision.agent.id"], "/", span["trace_id"])
		hashes = cache[cacheKey]
		if hashes == nil {
			rows, err := s.db.QueryContext(ctx, "SELECT id,sha256 FROM "+s.readModelTable("trace_media")+" WHERE org_id=? AND space_id=? AND agent_id=? AND trace_id=?", org, span["space_id"], resource["aivision.agent.id"], span["trace_id"])
			if err != nil {
				return err
			}
			hashes = map[string]string{}
			for rows.Next() {
				var id, hash string
				if err := rows.Scan(&id, &hash); err != nil {
					rows.Close()
					return err
				}
				hashes[id] = hash
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			cache[cacheKey] = hashes
		}
		for key, value := range parsed {
			encoded, err := json.Marshal(walk(value, 0))
			if err != nil {
				return err
			}
			attrs[key] = string(encoded)
		}
	}
	return nil
}
