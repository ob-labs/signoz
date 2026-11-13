package aivision

import (
	"context"
	"io"
	"net/http"
	"regexp"

	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/gorilla/mux"
)

var mediaTraceID = regexp.MustCompile(`^[0-9a-f]{32}$`)
var mediaSpanID = regexp.MustCompile(`^[0-9a-f]{16}$`)

func (h *handler) TraceMediaWrite(w http.ResponseWriter, r *http.Request) {
	org, ok := h.organization(w, r)
	if !ok {
		return
	}
	space, agent := r.URL.Query().Get("space_id"), r.URL.Query().Get("agent_uuid")
	if space == "" || agent == "" || len(space) > 255 || len(agent) > 255 {
		writeAPIError(w, 400, "bad_data", "space_id and agent_uuid required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.cfg.QueryTimeout)
	defer cancel()
	if r.Method == http.MethodDelete {
		id := mux.Vars(r)["mediaID"]
		if !traceMediaID.MatchString(id) {
			http.NotFound(w, r)
			return
		}
		// Explicit upload cleanup is scoped and cannot delete automatic trace media.
		_, err := h.backend.db.ExecContext(ctx, "DELETE FROM "+h.backend.readModelTable("trace_media")+" WHERE id=? AND org_id=? AND space_id=? AND agent_id=? AND NOT EXISTS (SELECT 1 FROM "+h.backend.tracesTable()+" WHERE org_id=? AND space_id=? AND trace_id="+h.backend.readModelTable("trace_media")+".trace_id AND span_id="+h.backend.readModelTable("trace_media")+".span_id)", id, org, space, agent, org, space)
		if err != nil {
			h.writeBackendError(w, err)
			return
		}
		w.WriteHeader(204)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, traceMediaMaxBytes+65536)
	if err := r.ParseMultipartForm(1 << 20); err != nil {
		writeTraceCapacityError(w, 413, "attachment request too large or invalid")
		return
	}
	if r.MultipartForm != nil {
		defer func() { _ = r.MultipartForm.RemoveAll() }()
	}
	trace, span, direction := r.FormValue("trace_id"), r.FormValue("span_id"), r.FormValue("direction")
	if !mediaTraceID.MatchString(trace) || !mediaSpanID.MatchString(span) || (direction != "input" && direction != "output") {
		writeAPIError(w, 400, "bad_data", "invalid media association")
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		writeAPIError(w, 400, "bad_data", "file required")
		return
	}
	defer file.Close()
	content, err := io.ReadAll(io.LimitReader(file, traceMediaMaxBytes+1))
	if err != nil {
		writeAPIError(w, 400, "bad_data", "cannot read attachment")
		return
	}
	if len(content) > traceMediaMaxBytes {
		writeTraceCapacityError(w, 413, "attachment exceeds 16 MiB")
		return
	}
	ref, err := ingest.StoreTraceMedia(ctx, h.backend.db, h.cfg.TablePrefix, org, space, agent, trace, span, direction, header.Header.Get("Content-Type"), content)
	if err != nil {
		writeAPIError(w, 400, "bad_data", err.Error())
		return
	}
	writeJSON(w, 201, ref)
}
