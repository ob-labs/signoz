package aivision

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/SigNoz/signoz/pkg/ingest"
	"io"
	"math"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const traceReadModelContract = "native-trace-read-model-v1"
const traceMetricsSupplementVersion = "native-trace-metrics-v1"

var projectionContextPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var readModelTraceID = regexp.MustCompile(`^[0-9a-f]{1,32}$`)

func readModelRequestTimeout(action string, queryTimeout time.Duration) time.Duration {
	if action == "maintenance/resume" {
		return ingest.ProjectionRecoveryTimeout
	}
	return queryTimeout
}

type traceProjection struct {
	ProjectionContext     string          `json:"projection_context"`
	SpaceID               string          `json:"space_id"`
	TraceID               string          `json:"trace_id"`
	SourceRevision        int64           `json:"source_revision"`
	MaintenanceGeneration int64           `json:"maintenance_generation"`
	SemanticsVersion      int             `json:"semantics_version"`
	AgentUUID             string          `json:"agent_uuid"`
	AgentProduct          string          `json:"agent_product"`
	UserID                string          `json:"user_id"`
	Period                int64           `json:"period"`
	Duration              *float64        `json:"duration"`
	Status                int             `json:"status"`
	NodeCount             int64           `json:"node_count"`
	Environment           *string         `json:"environment"`
	Scene                 string          `json:"scene"`
	SceneValues           []string        `json:"scene_values"`
	Username              *string         `json:"username"`
	SessionID             string          `json:"session_id"`
	Name                  string          `json:"name"`
	Model                 string          `json:"model"`
	SearchText            string          `json:"search_text"`
	InputText             string          `json:"input_text"`
	OutputText            string          `json:"output_text"`
	Tags                  []string        `json:"tags"`
	Row                   json.RawMessage `json:"row"`
	Preview               json.RawMessage `json:"preview"`
}

func validateTraceProjection(p traceProjection) error {
	if !projectionContextPattern.MatchString(p.ProjectionContext) || p.SpaceID == "" || len(p.SpaceID) > 255 || !readModelTraceID.MatchString(p.TraceID) || p.SourceRevision < 1 || p.MaintenanceGeneration < 1 || p.SemanticsVersion != 1 || p.AgentUUID == "" || len(p.AgentUUID) > 255 {
		return fmt.Errorf("invalid projection scope, revision or semantics_version")
	}
	if p.Period < 0 || p.Period > math.MaxInt64/1000000 || p.Duration != nil && (*p.Duration < 0 || math.IsNaN(*p.Duration) || math.IsInf(*p.Duration, 0)) || p.NodeCount < 0 || p.NodeCount > 9007199254740991 {
		return fmt.Errorf("invalid projection summary")
	}
	for _, s := range []string{p.AgentProduct, p.UserID, p.Scene, p.SessionID, p.Model} {
		if len(s) > 255 {
			return fmt.Errorf("invalid projection dimension length")
		}
	}
	for _, value := range []*string{p.Username, p.Environment} {
		if value != nil && len(*value) > 255 {
			return fmt.Errorf("invalid projection dimension length")
		}
	}
	if len(p.Name) > 512 || p.Status < 0 || p.Status > 2 || len(p.Tags) > 1000 || len(p.SceneValues) > 1000 {
		return fmt.Errorf("invalid projection dimensions")
	}
	row, rowErr := tracePageJSON(string(p.Row))
	preview, previewErr := tracePageJSON(string(p.Preview))
	if rowErr != nil || row == nil || previewErr != nil || preview == nil {
		return fmt.Errorf("invalid projection JSON object")
	}
	if row["trace_id"] != p.TraceID {
		return fmt.Errorf("invalid canonical trace identity")
	}
	if _, _, err := projectionTimes(p); err != nil {
		return err
	}
	if preview["version"] != "native-trace-preview-v1" || preview["complete"] != true {
		return fmt.Errorf("invalid projection preview contract")
	}
	for _, key := range []string{"inputTruncated", "outputTruncated"} {
		if _, ok := preview[key].(bool); !ok {
			return fmt.Errorf("invalid projection preview truncation")
		}
	}
	for _, key := range []string{"input", "output"} {
		value, present := preview[key]
		if !present {
			return fmt.Errorf("invalid projection preview text")
		}
		if value == nil {
			continue
		}
		text, ok := value.(string)
		if !ok || utf8.RuneCountInString(text) > 1024 {
			return fmt.Errorf("invalid projection preview text")
		}
	}
	if root, ok := preview["rootObservationId"].(string); !ok || len(root) > 255 {
		return fmt.Errorf("invalid projection preview root")
	}

	if _, ok := row["spans"]; ok {
		return fmt.Errorf("invalid projection: spans are not a canonical row")
	}
	if complete, present := row["_contentComplete"]; present {
		if _, ok := complete.(bool); !ok {
			return fmt.Errorf("invalid projection content completeness")
		}
	}
	if previews, present := row["_operation_previews"]; present {
		values, ok := previews.(map[string]any)
		if !ok || int64(len(values)) > p.NodeCount {
			return fmt.Errorf("invalid projection operation previews")
		}
		for id, value := range values {
			preview, ok := value.(map[string]any)
			if id == "" || len(id) > 255 || !ok || validateOperationPreview(preview) != nil {
				return fmt.Errorf("invalid projection operation preview")
			}
		}
	}
	for _, tag := range append(append([]string{}, p.Tags...), p.SceneValues...) {
		if len(tag) > 1024 {
			return fmt.Errorf("invalid projection tag")
		}
	}
	return nil
}

func projectionTimes(p traceProjection) (uint64, any, error) {
	row, err := tracePageJSON(string(p.Row))
	if err != nil {
		return 0, nil, err
	}
	start, err := strconv.ParseUint(fmt.Sprint(row["start_time_unix_nano"]), 10, 64)
	if err != nil || int64(start/1000000) != p.Period {
		return 0, nil, fmt.Errorf("invalid canonical start timestamp")
	}
	var end any
	if value := row["end_time_unix_nano"]; value != nil {
		parsed, err := strconv.ParseUint(fmt.Sprint(value), 10, 64)
		if err != nil {
			return 0, nil, fmt.Errorf("invalid canonical end timestamp")
		}
		end = parsed
	}
	return start, end, nil
}
func (s *oceanBaseStore) readModelTable(suffix string) string {
	return "`" + s.cfg.TablePrefix + "_" + suffix + "`"
}
func readModelUnsupported(reason string) map[string]any {
	return map[string]any{"supported": false, "reason": reason}
}
func (s *oceanBaseStore) readModelUnavailable() string {
	if s.db == nil {
		return "transactional_store_unavailable"
	}
	return ""
}

func (h *handler) ReadModel(w http.ResponseWriter, r *http.Request) {
	org, ok := h.organization(w, r)
	if !ok {
		return
	}
	action := strings.TrimPrefix(r.URL.Path, "/api/v1/ai-vision/traces/read-model/")
	if action == "capabilities" {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		if r.URL.RawQuery != "" {
			writeAPIError(w, http.StatusBadRequest, "bad_data", "capabilities does not accept query parameters")
			return
		}
		// This describes the server protocol, independently of database health
		// or optional schema readiness. Actual readers retain their own gates.
		writeJSON(w, http.StatusOK, map[string]any{"supported": true, "contract": "native-trace-performance-capabilities-v1", "content_projection": "native-trace-content-v1", "operation_facts": true, "source_pages": true})
		return
	}
	if reason := h.backend.readModelUnavailable(); reason != "" {
		writeJSON(w, 200, readModelUnsupported(reason))
		return
	}
	switch action {
	case "source-page", "page", "window", "metrics-window", "metrics-aggregate", "operation-page", "batch-snapshot", "sessions/page", "snapshot":
		if !acquireTraceQuery(w) {
			return
		}
		defer releaseTraceQuery()
	}
	ctx, cancel := context.WithTimeout(r.Context(), readModelRequestTimeout(action, h.cfg.QueryTimeout))
	defer cancel()
	var result any
	var err error
	switch action {
	case "source-page":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		for key, values := range r.URL.Query() {
			if (key != "space_id" && key != "trace_id" && key != "cursor") || len(values) != 1 {
				err = fmt.Errorf("invalid source page parameter: %s", key)
				break
			}
		}
		if err == nil {
			result, err = h.backend.traceSourcePage(ctx, org, r.URL.Query().Get("space_id"), r.URL.Query().Get("trace_id"), r.URL.Query().Get("cursor"))
		}
	case "operation-page":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		var p nativeListParams
		var limit int
		p, limit, err = h.operationPageParams(r)
		if err == nil {
			result, err = h.backend.traceOperationPage(ctx, org, p, limit)
		}
	case "metrics-aggregate":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		var p nativeListParams
		var bucket, group string
		p, bucket, group, err = h.statisticsParams(r)
		if err == nil {
			result, err = h.backend.traceMetricsAggregate(ctx, org, p, bucket, group)
		}
	case "batch-snapshot", "batch-publish":
		method := http.MethodGet
		if action == "batch-publish" {
			method = http.MethodPost
		}
		if r.Method != method {
			methodNotAllowed(w, method)
			return
		}
		result, err = h.projectionBatchAction(r, ctx, org, action)
	case "window", "metrics-window":
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		var p nativeListParams
		var raw bool
		p, raw, err = h.traceWindowParams(r)
		if err == nil {
			if action == "metrics-window" {
				result, err = h.backend.traceMetricsWindow(ctx, org, p, raw)
			} else {
				result, err = h.backend.traceWindow(ctx, org, p, raw)
			}
		}
	case "maintenance/resume":
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		if !projectionResumeOperatorAuthorized(r) {
			writeAPIError(w, http.StatusForbidden, "forbidden", "deployment operator authorization required")
			return
		}
		if !ingest.ProjectionMaintenanceEnabled() {
			result = readModelUnsupported("projection_maintenance_disabled")
		} else {
			err = ingest.ResumeProjectionMaintenance(ctx, h.backend.db, h.backend.cfg.TablePrefix)
			if err == nil {
				result = map[string]any{"supported": true, "resumed": true}
				// Restore existing statistics before optional content work can
				// consume the shared operator-request deadline.
				if optionalErr := ingest.InitializeTraceStatistics(ctx, h.backend.db, h.backend.cfg.TablePrefix); optionalErr != nil && h.logger != nil {
					h.logger.Warn("optional statistics initialization after maintenance resume failed; complete metrics fallback retained", "error", optionalErr)
				}
				if optionalErr := ingest.InitializeTraceContentColumn(ctx, h.backend.db, h.backend.cfg.TablePrefix); optionalErr != nil && h.logger != nil {
					h.logger.Warn("optional content coverage initialization after maintenance resume failed; JSON coverage retained", "error", optionalErr)
				}
				if optionalErr := ingest.InitializeTraceProjectionSummaries(ctx, h.backend.db, h.backend.cfg.TablePrefix); optionalErr != nil && h.logger != nil {
					h.logger.Warn("projection summary initialization after maintenance resume failed; list queries remain unavailable", "error", optionalErr)
				}
			}
		}
	case "sessions/page":
		var p sessionReadModelParams
		p, err = h.sessionReadModelParams(r)
		if err == nil {
			result, err = h.backend.sessionReadModelPage(ctx, org, p)
		}
	case "publish":
		var p traceProjection
		if err = decodeReadModelJSON(r, &p); err == nil {
			err = validateTraceProjection(p)
		}
		if err == nil {
			result, err = h.backend.publishTraceProjection(ctx, org, p)
		}
	case "backfill":
		var p struct {
			SpaceID string `json:"space_id"`
			Limit   int    `json:"limit"`
		}
		err = decodeReadModelJSON(r, &p)
		if err == nil && (p.SpaceID == "" || len(p.SpaceID) > 255 || p.Limit < 1 || p.Limit > 500) {
			err = fmt.Errorf("invalid backfill scope or limit")
		}
		if err == nil {
			result, err = h.backend.backfillTraceSources(ctx, org, p.SpaceID, p.Limit)
		}
	case "snapshot", "dirty":
		space := r.URL.Query().Get("space_id")
		trace := r.URL.Query().Get("trace_id")
		if space == "" || len(space) > 255 {
			err = fmt.Errorf("invalid space_id")
			break
		}
		if action == "snapshot" {
			if !readModelTraceID.MatchString(trace) {
				err = fmt.Errorf("invalid trace_id")
				break
			}
			result, err = h.backend.traceSourceSnapshot(ctx, org, space, trace)
		} else {
			limit := 100
			if raw := r.URL.Query().Get("limit"); raw != "" {
				limit, _ = strconv.Atoi(raw)
			}
			if limit < 1 || limit > 500 {
				err = fmt.Errorf("invalid dirty limit")
				break
			}
			requiredSupplement := ""
			if values, present := r.URL.Query()["required_supplement"]; present {
				if len(values) != 1 || values[0] != traceMetricsSupplementVersion {
					err = fmt.Errorf("invalid required_supplement version")
					break
				}
				requiredSupplement = values[0]
			}
			projectionContext := r.URL.Query().Get("projection_context")
			if !projectionContextPattern.MatchString(projectionContext) {
				writeJSON(w, 200, readModelUnsupported("projection_context_required"))
				return
			}
			after := r.URL.Query().Get("after_trace_id")
			if after != "" && !readModelTraceID.MatchString(after) {
				err = fmt.Errorf("invalid after_trace_id")
				break
			}
			requiredStatistics := ""
			if values, present := r.URL.Query()["required_statistics"]; present {
				if len(values) != 1 || values[0] != traceStatisticsVersion {
					err = fmt.Errorf("invalid required_statistics version")
					break
				}
				requiredStatistics = values[0]
			}
			result, err = h.backend.dirtyTraceSources(ctx, org, space, projectionContext, after, limit, requiredSupplement, requiredStatistics)
		}
	case "page":
		allowed := map[string]bool{}
		for _, key := range []string{"from", "to", "space_id", "agent_uuid", "agent_product", "user", "page", "limit", "orderBy", "order_by", "filters", "keyword", "searchType", "search_type", "input_keyword", "tags", "env", "scene", "username", "trace_id", "projection_context", "metadata_only"} {
			allowed[key] = true
		}
		for key := range r.URL.Query() {
			if !allowed[key] {
				writeJSON(w, 200, readModelUnsupported("unsupported_parameter:"+key))
				return
			}
		}
		var p nativeListParams
		p, err = h.nativeParams(r)
		if err == nil {
			q := r.URL.Query()
			if values, present := q["metadata_only"]; present {
				if len(values) != 1 || (values[0] != "true" && values[0] != "false") {
					err = fmt.Errorf("metadata_only must be true or false")
				} else {
					p.MetadataOnly = values[0] == "true"
				}
			}
			p.ProjectionContext = q.Get("projection_context")
			p.AgentUUID = q.Get("agent_uuid")
			p.TraceID = q.Get("trace_id")
			p.Keyword = q.Get("keyword")
			p.InputKeyword = q.Get("input_keyword")
			p.Env = q.Get("env")
			p.Scene = q.Get("scene")
			p.Username = q.Get("username")
			if raw := q.Get("filters"); raw != "" {
				d := json.NewDecoder(strings.NewReader(raw))
				d.UseNumber()
				err = d.Decode(&p.Filters)
			}
			if values, exists := q["tags"]; exists {
				p.Tags = values
				if len(values) == 1 && strings.HasPrefix(strings.TrimSpace(values[0]), "[") {
					err = json.Unmarshal([]byte(values[0]), &p.Tags)
				}
			}
			if err == nil {
				result, err = h.backend.readModelPage(ctx, org, p)
			}
		}
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		h.writeBackendError(w, err)
		return
	}
	writeJSON(w, 200, result)
}
func decodeReadModelJSON(r *http.Request, v any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, (16<<20)+1))
	if err != nil || len(body) > 16<<20 {
		return fmt.Errorf("invalid read model payload size")
	}
	d := json.NewDecoder(strings.NewReader(string(body)))
	d.DisallowUnknownFields()
	d.UseNumber()
	if err := d.Decode(v); err != nil {
		return fmt.Errorf("invalid read model payload: %w", err)
	}
	var trailing any
	if d.Decode(&trailing) != io.EOF {
		return fmt.Errorf("invalid trailing JSON")
	}
	return nil
}
func (s *oceanBaseStore) beginReadModel(ctx context.Context, readOnly bool) (*sql.Tx, error) {
	isolation := sql.LevelReadCommitted
	if readOnly {
		isolation = sql.LevelRepeatableRead
	}
	return s.db.BeginTx(ctx, &sql.TxOptions{Isolation: isolation, ReadOnly: readOnly})
}

func scanReadModelRows(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]map[string]any, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	result := []map[string]any{}
	for rows.Next() {
		values := make([]any, len(columns))
		refs := make([]any, len(columns))
		for i := range values {
			refs[i] = &values[i]
		}
		if err := rows.Scan(refs...); err != nil {
			return nil, err
		}
		row := map[string]any{}
		for i, c := range columns {
			v := values[i]
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			row[c] = v
		}
		result = append(result, row)
	}
	return result, rows.Err()
}
func (s *oceanBaseStore) traceSourceSnapshot(ctx context.Context, org, space, trace string) (any, error) {
	tx, err := s.beginReadModel(ctx, true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	enabled, generation, err := ingest.ReadProjectionMaintenance(ctx, tx, s.cfg.TablePrefix)
	if err != nil {
		return nil, err
	}
	if !enabled {
		return readModelUnsupported("projection_maintenance_unavailable"), nil
	}
	var revision, min, max int64
	var sourceUser, sourceProduct string
	var scopeValid bool
	err = tx.QueryRowContext(ctx, "SELECT source_revision,min_timestamp,max_timestamp,user_id,agent_product,scope_valid FROM "+s.readModelTable("trace_sources")+" WHERE org_id=? AND space_id=? AND trace_id=?", org, space, trace).Scan(&revision, &min, &max, &sourceUser, &sourceProduct, &scopeValid)
	if err == sql.ErrNoRows {
		return readModelUnsupported("source_not_enrolled"), nil
	}
	if err != nil {
		return nil, err
	}
	var count, sourceBytes, maxSpanBytes int64
	err = tx.QueryRowContext(ctx, "SELECT COUNT(*),COALESCE(SUM("+nativeSpanSourceBytesSQL()+"),0),COALESCE(MAX("+nativeSpanSourceBytesSQL()+"),0) FROM "+s.tracesTable()+" WHERE org_id=? AND space_id=? AND trace_id=?", org, space, trace).Scan(&count, &sourceBytes, &maxSpanBytes)
	if err != nil {
		return nil, err
	}
	if count == 0 || count > 100000 {
		return readModelUnsupported("source_empty_or_too_large"), nil
	}
	if sourceBytes > projectionBatchPayloadLimit || maxSpanBytes > traceSpanSourceByteLimit {
		return readModelUnsupported("source_snapshot_payload_limit"), nil
	}
	rows := []map[string]any{}
	wireBytes := 0
	err = scanReadModelRowsEach(ctx, tx, "SELECT "+nativeSpanProjection+" FROM "+s.tracesTable()+" WHERE org_id=? AND space_id=? AND trace_id=? ORDER BY timestamp,span_id", []any{org, space, trace}, func(row map[string]any) error {
		if err := decodeWindowSpan(row); err != nil {
			return tracePageUnsupported{"source_json_invalid"}
		}
		if err := compactNativeSpan(row); err != nil {
			return err
		}
		size, err := encodedSpanBytes(row)
		if err != nil {
			return err
		}
		wireBytes += size
		if wireBytes > projectionBatchPayloadLimit-4096 {
			return tracePageUnsupported{"source_snapshot_payload_limit"}
		}
		rows = append(rows, row)
		return nil
	})
	if unsupported, ok := err.(tracePageUnsupported); ok {
		return readModelUnsupported(unsupported.reason), nil
	}
	if err != nil {
		return nil, err
	}
	if int64(len(rows)) != count {
		return readModelUnsupported("source_snapshot_incomplete"), nil
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return map[string]any{"supported": true, "source_user_id": sourceUser, "source_agent_product": sourceProduct, "scope_valid": scopeValid, "contract": "native-trace-source-v1", "trace_id": trace, "source_revision": revision, "maintenance_generation": generation, "span_count": count, "min_timestamp": strconv.FormatInt(min, 10), "max_timestamp": strconv.FormatInt(max, 10), "complete": true, "spans": rows}, nil
}
func (s *oceanBaseStore) dirtyTraceSources(ctx context.Context, org, space, projectionContext, after string, limit int, requiredSupplement string, requiredStatistics ...string) (any, error) {
	if requiredSupplement != "" && requiredSupplement != traceMetricsSupplementVersion {
		return nil, fmt.Errorf("invalid required_supplement version")
	}
	tx, err := s.beginReadModel(ctx, true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if reason, err := s.projectionMaintenanceReason(ctx, tx, false); err != nil {
		return nil, err
	} else if reason != "" {
		return readModelUnsupported(reason), nil
	}
	statisticsReady := false
	if len(requiredStatistics) > 0 && requiredStatistics[0] != "" {
		if requiredStatistics[0] != traceStatisticsVersion {
			return nil, fmt.Errorf("invalid required_statistics version")
		}
		statisticsReady, err = ingest.TraceStatisticsSchemaReady(ctx, tx, s.cfg.TablePrefix)
		if err != nil {
			return nil, err
		}
	}
	if requiredSupplement != "" && !statisticsReady {
		ready, err := ingest.TraceProjectionMetricsVersionReady(ctx, tx, s.cfg.TablePrefix)
		if err != nil {
			return nil, err
		}
		if !ready {
			return readModelUnsupported("projection_metrics_version_unavailable"), nil
		}
	}
	join := ""
	if statisticsReady {
		join = " LEFT JOIN " + s.readModelTable("trace_statistics") + " t ON t.org_id=p.org_id AND t.space_id=p.space_id AND t.trace_id=p.trace_id"
	}
	query := "SELECT s.trace_id,s.source_revision FROM " + s.readModelTable("trace_sources") + " s LEFT JOIN " + s.readModelTable("trace_projections") + " p ON p.org_id=s.org_id AND p.space_id=s.space_id AND p.trace_id=s.trace_id" + join + " WHERE s.org_id=? AND s.space_id=? AND (s.scope_valid=FALSE OR s.dirty=TRUE OR p.trace_id IS NULL OR p.source_revision<>s.source_revision OR p.semantics_version<>1 OR p.projection_context<>?"
	args := []any{org, space, projectionContext}
	if statisticsReady {
		query += " OR t.trace_id IS NULL OR t.source_revision<>p.source_revision OR BINARY t.projection_context<>BINARY p.projection_context OR p.statistics_receipt_v1 IS NULL OR BINARY t.receipt<>BINARY p.statistics_receipt_v1"
	} else if requiredSupplement != "" {
		// Supplements can be populated incrementally without invalidating ready
		// Trace/Session summaries or changing their source revisions. COALESCE
		// includes absent JSON paths; JSON null also differs from the version.
		query += " OR BINARY COALESCE(p.metrics_version_v1,'')<>BINARY ?"
		args = append(args, requiredSupplement)
	}
	query += ") AND s.trace_id>? ORDER BY BINARY s.trace_id LIMIT ?"
	args = append(args, after, limit+1)
	rows, err := scanReadModelRows(ctx, tx, query, args...)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	cursor := ""
	if len(rows) > 0 {
		cursor = fmt.Sprint(rows[len(rows)-1]["trace_id"])
	}
	result := map[string]any{"supported": true, "data": rows, "has_more": hasMore, "next_cursor": cursor}
	if requiredSupplement != "" {
		result["required_supplement"] = requiredSupplement
	}
	if statisticsReady {
		result["required_statistics"] = traceStatisticsVersion
	}
	return result, nil
}
func (s *oceanBaseStore) publishTraceProjection(ctx context.Context, org string, p traceProjection) (any, error) {
	tx, err := s.beginReadModel(ctx, false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	enabled, generation, err := ingest.LockProjectionMaintenance(ctx, tx, s.cfg.TablePrefix)
	if err != nil {
		return nil, err
	}
	if !enabled {
		return readModelUnsupported("projection_maintenance_unavailable"), nil
	}
	receipt, err := s.publishTraceProjectionInTx(ctx, tx, org, p, generation)
	if err != nil {
		return nil, err
	}
	if receipt["published"] != true {
		return receipt, nil
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return receipt, nil
}

// The caller must hold the shared maintenance fence in this write transaction.
// Single and batch publication use the identical source scope/revision CAS.
func (s *oceanBaseStore) publishTraceProjectionInTx(ctx context.Context, tx *sql.Tx, org string, p traceProjection, generation int64, statisticsCapability ...bool) (map[string]any, error) {
	if p.MaintenanceGeneration != generation {
		return readModelUnsupported("maintenance_generation_changed"), nil
	}
	var revision int64
	var sourceUser, sourceProduct string
	var scopeValid bool
	err := tx.QueryRowContext(ctx, "SELECT source_revision,user_id,agent_product,scope_valid FROM "+s.readModelTable("trace_sources")+" WHERE org_id=? AND space_id=? AND trace_id=? FOR UPDATE", org, p.SpaceID, p.TraceID).Scan(&revision, &sourceUser, &sourceProduct, &scopeValid)
	if err == sql.ErrNoRows {
		return readModelUnsupported("source_not_enrolled"), nil
	}
	if err != nil {
		return nil, err
	}
	if !scopeValid || p.UserID != sourceUser || p.AgentProduct != sourceProduct {
		return readModelUnsupported("source_scope_unprojectable"), nil
	}
	if revision != p.SourceRevision {
		return readModelUnsupported("source_revision_changed"), nil
	}
	scenes, _ := json.Marshal(p.SceneValues)
	if p.SceneValues == nil {
		scenes = []byte("[]")
	}
	tags, _ := json.Marshal(p.Tags)
	if p.Tags == nil {
		tags = []byte("[]")
	}
	startNS, endNS, err := projectionTimes(p)
	if err != nil {
		return nil, err
	}
	columns := []string{"source_revision", "semantics_version", "projection_context", "start_ns", "end_ns", "agent_uuid", "agent_product", "user_id", "period", "duration", "status", "node_count", "environment", "scene", "scene_values_json", "username", "session_id", "name", "model", "search_text", "input_text", "output_text", "tags_json", "row_json", "preview_json"}
	args := []any{org, p.SpaceID, p.TraceID, p.SourceRevision, p.SemanticsVersion, p.ProjectionContext, startNS, endNS, p.AgentUUID, p.AgentProduct, p.UserID, p.Period, p.Duration, p.Status, p.NodeCount, p.Environment, p.Scene, string(scenes), p.Username, p.SessionID, p.Name, p.Model, p.SearchText, p.InputText, p.OutputText, string(tags), string(p.Row), string(p.Preview)}
	updates := []string{}
	for _, column := range columns {
		updates = append(updates, column+"=VALUES("+column+")")
	}
	query := "INSERT INTO " + s.readModelTable("trace_projections") + " (org_id,space_id,trace_id," + strings.Join(columns, ",") + ") VALUES (" + strings.TrimSuffix(strings.Repeat("?,", len(args)), ",") + ") ON DUPLICATE KEY UPDATE " + strings.Join(updates, ",")
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return nil, err
	}
	if err := s.publishStatistics(ctx, tx, org, p, statisticsCapability...); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE "+s.readModelTable("trace_sources")+" SET dirty=FALSE WHERE org_id=? AND space_id=? AND trace_id=? AND source_revision=?", org, p.SpaceID, p.TraceID, p.SourceRevision); err != nil {
		return nil, err
	}
	return map[string]any{"supported": true, "published": true, "source_revision": revision, "maintenance_generation": generation}, nil
}

const traceBackfillChunkLimit = 25
const traceBackfillWorkBudget = 2 * time.Second

type traceBackfillProgress struct {
	after      string
	complete   bool
	enrolled   int
	generation int64
}

func (p traceBackfillProgress) response() map[string]any {
	return map[string]any{"supported": true, "after_trace_id": p.after, "complete": p.complete, "enrolled": p.enrolled}
}

// A completed coverage cursor is a read-only fact in the same RR snapshot as
// the maintenance fence. Empty worker polls need not queue behind raw writers.
func (s *oceanBaseStore) completedTraceBackfill(ctx context.Context, org, space string) (traceBackfillProgress, string, error) {
	progress := traceBackfillProgress{}
	tx, err := s.beginReadModel(ctx, true)
	if err != nil {
		return progress, "", err
	}
	defer tx.Rollback()
	enabled, generation, err := ingest.ReadProjectionMaintenance(ctx, tx, s.cfg.TablePrefix)
	if err != nil {
		return progress, "", err
	}
	if !enabled {
		return progress, "projection_maintenance_unavailable", nil
	}
	progress.generation = generation
	err = tx.QueryRowContext(ctx, "SELECT after_trace_id,complete FROM "+s.readModelTable("trace_projection_coverage")+" WHERE org_id=? AND space_id=?", org, space).Scan(&progress.after, &progress.complete)
	if err == sql.ErrNoRows {
		return progress, "", nil
	}
	if err != nil {
		return progress, "", err
	}
	if progress.complete {
		err = tx.Commit()
	}
	return progress, "", err
}

func (s *oceanBaseStore) backfillTraceSources(ctx context.Context, org, space string, limit int) (any, error) {
	if limit < 1 || limit > 500 {
		return nil, fmt.Errorf("invalid backfill limit")
	}
	started := time.Now()
	progress, reason, err := s.completedTraceBackfill(ctx, org, space)
	if err != nil {
		return nil, err
	}
	if reason != "" {
		return readModelUnsupported(reason), nil
	}
	for !progress.complete && progress.enrolled < limit {
		chunkLimit := limit - progress.enrolled
		if chunkLimit > traceBackfillChunkLimit {
			chunkLimit = traceBackfillChunkLimit
		}
		chunk, reason, err := s.backfillTraceSourceChunk(ctx, org, space, progress.generation, chunkLimit)
		if err != nil {
			return nil, err
		}
		if reason != "" {
			return readModelUnsupported(reason), nil
		}
		progress.after, progress.complete = chunk.after, chunk.complete
		progress.enrolled += chunk.enrolled
		// Only committed work is reported. The budget is checked outside the
		// transaction, so a cursor can never jump past rolled-back sources.
		if time.Since(started) >= traceBackfillWorkBudget {
			break
		}
	}
	return progress.response(), nil
}

func (s *oceanBaseStore) backfillTraceSourceChunk(ctx context.Context, org, space string, generation int64, limit int) (traceBackfillProgress, string, error) {
	progress := traceBackfillProgress{generation: generation}
	tx, err := s.beginReadModel(ctx, false)
	if err != nil {
		return progress, "", err
	}
	defer tx.Rollback()
	enabled, currentGeneration, err := ingest.LockProjectionMaintenance(ctx, tx, s.cfg.TablePrefix)
	if err != nil {
		return progress, "", err
	}
	if !enabled {
		return progress, "projection_maintenance_unavailable", nil
	}
	if currentGeneration != generation {
		return progress, "maintenance_generation_changed", nil
	}
	coverage := s.readModelTable("trace_projection_coverage")
	if _, err := tx.ExecContext(ctx, "INSERT IGNORE INTO "+coverage+" (org_id,space_id) VALUES (?,?)", org, space); err != nil {
		return progress, "", err
	}
	if err := tx.QueryRowContext(ctx, "SELECT after_trace_id,complete FROM "+coverage+" WHERE org_id=? AND space_id=? FOR UPDATE", org, space).Scan(&progress.after, &progress.complete); err != nil {
		return progress, "", err
	}
	if !progress.complete {
		rows, err := scanReadModelRows(ctx, tx, "SELECT trace_id,MIN(timestamp) AS min_timestamp,MAX(timestamp) AS max_timestamp,MIN(user_id) AS user_id,MIN(agent_product) AS agent_product,(COUNT(DISTINCT BINARY user_id)=1 AND MIN(user_id)<>'' AND COUNT(DISTINCT BINARY agent_product)=1 AND MIN(agent_product)<>'') AS scope_valid FROM "+s.tracesTable()+" WHERE org_id=? AND space_id=? AND trace_id>? GROUP BY trace_id ORDER BY trace_id LIMIT ?", org, space, progress.after, limit)
		if err != nil {
			return progress, "", err
		}
		// Ingest and publication take this same fence before changing source
		// revisions. Validate the complete bounded page, then lock/upsert all of
		// its sources in one statement instead of two round trips per Trace.
		values := make([]any, 0, len(rows)*8)
		placeholders := make([]string, 0, len(rows))
		after := progress.after
		for _, row := range rows {
			id := fmt.Sprint(row["trace_id"])
			if !readModelTraceID.MatchString(id) || id <= after {
				return progress, "", fmt.Errorf("invalid historical trace_id")
			}
			min, minErr := asInt64(row["min_timestamp"])
			max, maxErr := asInt64(row["max_timestamp"])
			user, userOK := row["user_id"].(string)
			product, productOK := row["agent_product"].(string)
			valid, validOK := row["scope_valid"].(bool)
			if !validOK {
				value, err := asInt64(row["scope_valid"])
				validOK = err == nil && (value == 0 || value == 1)
				valid = value == 1
			}
			if minErr != nil || maxErr != nil || min < 0 || max < min || !userOK || !productOK || !validOK {
				return progress, "", fmt.Errorf("invalid historical trace summary")
			}
			placeholders = append(placeholders, "(?,?,?,1,?,?,?,?,?,TRUE)")
			values = append(values, org, space, id, min, max, user, product, valid)
			after = id
		}
		if len(rows) > 0 {
			// New sources begin at revision 1, exactly matching the former
			// revision-0 placeholder plus increment. Existing sources advance
			// exactly once; stale publishers still fail their revision CAS.
			query := "INSERT INTO " + s.readModelTable("trace_sources") + " (org_id,space_id,trace_id,source_revision,min_timestamp,max_timestamp,user_id,agent_product,scope_valid,dirty) VALUES " + strings.Join(placeholders, ",") + " ON DUPLICATE KEY UPDATE source_revision=source_revision+1,dirty=TRUE,min_timestamp=VALUES(min_timestamp),max_timestamp=VALUES(max_timestamp),user_id=VALUES(user_id),agent_product=VALUES(agent_product),scope_valid=VALUES(scope_valid)"
			if _, err := tx.ExecContext(ctx, query, values...); err != nil {
				return progress, "", err
			}
			progress.after, progress.enrolled = after, len(rows)
		}
		progress.complete = len(rows) < limit
		if _, err := tx.ExecContext(ctx, "UPDATE "+coverage+" SET after_trace_id=?,complete=? WHERE org_id=? AND space_id=?", progress.after, progress.complete, org, space); err != nil {
			return progress, "", err
		}
	}
	if err := tx.Commit(); err != nil {
		return progress, "", err
	}
	return progress, "", nil
}

func readModelWhere(org string, p nativeListParams) (string, []any, error) {
	if !projectionContextPattern.MatchString(p.ProjectionContext) {
		return "", nil, tracePageUnsupported{"projection_context_required"}
	}
	if p.SpaceID == "" || p.AgentUUID == "" || len(p.AgentUUID) > 255 || len(p.User) > 255 || len(p.AgentProduct) > 255 || len(p.SpaceID) > 255 {
		return "", nil, tracePageUnsupported{"canonical_agent_uuid_required"}
	}
	if p.SessionID != "" || len(p.TraceIDs) > 0 || len(p.SessionIDs) > 0 || len(p.Fields) > 0 {
		return "", nil, tracePageUnsupported{"unsupported_scope_or_fields"}
	}
	if p.Page < 1 || p.Limit < 1 || p.Limit > 200 || p.Page > 1000000 || p.From < 0 || p.To <= p.From || p.To > math.MaxInt64/1000000 {
		return "", nil, tracePageUnsupported{"unsupported_page_or_window"}
	}
	where := []string{"p.org_id=?", "p.space_id=?", "p.agent_uuid=?", "p.start_ns>=?", "p.start_ns<?", "BINARY p.agent_uuid=BINARY ?"}
	args := []any{org, p.SpaceID, p.AgentUUID, p.From * 1000000, p.To * 1000000, p.AgentUUID}
	equal := func(column, value string) {
		if value != "" {
			where = append(where, "BINARY p."+column+"=BINARY ?")
			args = append(args, value)
		}
	}
	equal("agent_product", p.AgentProduct)
	equal("user_id", p.User)
	equal("environment", p.Env)
	equal("username", p.Username)
	if p.TraceID != "" {
		where = append(where, "BINARY p.trace_id=BINARY ?")
		args = append(args, p.TraceID)
	}
	if p.Scene != "" {
		value, _ := json.Marshal(p.Scene)
		where = append(where, "JSON_CONTAINS(p.scene_values_json,?)")
		args = append(args, string(value))
	}
	if len(p.Tags) > 0 {
		clauses := []string{}
		for _, tag := range p.Tags {
			value, _ := json.Marshal(tag)
			clauses = append(clauses, "JSON_CONTAINS(p.tags_json,?)")
			args = append(args, string(value))
		}
		where = append(where, "("+strings.Join(clauses, " OR ")+")")
	}

	// Python sends its exact .lower() normalization. BINARY LOCATE preserves
	// literal %, _, slash and unicode characters rather than LIKE/collation rules.
	if p.Keyword != "" {
		column := "search_text"
		switch p.SearchType {
		case "", "traceId":
			column = "trace_id"
		case "input":
			column = "input_text"
		case "output":
			column = "output_text"
		default:
			return "", nil, tracePageUnsupported{"unsupported_search_type"}
		}
		where = append(where, "LOCATE(BINARY ?,BINARY p."+column+")>0")
		args = append(args, p.Keyword)
	}
	if p.InputKeyword != "" {
		where = append(where, "LOCATE(BINARY ?,BINARY p.input_text)>0")
		args = append(args, p.InputKeyword)
	}
	columns := map[string]string{"duration": "duration", "duration_ms": "duration", "latency": "duration", "node_count": "node_count", "nodeCount": "node_count", "span_count": "node_count", "status_code": "status", "status": "status", "userId": "username", "environment": "environment", "name": "name", "period": "period", "trace_id": "trace_id"}
	for _, filter := range p.Filters {
		if filter.Field == "total_tokens" || filter.Field == "attributes.gen_ai.usage.total_tokens" {
			// Session helper tool-only zero and canonical unknown both fail a
			// positive minimum. No such equivalence is claimed for =0/IS_NULL.
			number, err := strconv.ParseInt(fmt.Sprint(filter.Value), 10, 64)
			if err != nil || filter.Op != ">=" || number < 100 || number > 9007199254740991 {
				return "", nil, tracePageUnsupported{"unsupported_token_minimum"}
			}
			where = append(where, "p."+sessionTokenGeneratedValueColumn+">=?")
			args = append(args, number)
			continue
		}
		column, ok := columns[filter.Field]
		if !ok {
			return "", nil, tracePageUnsupported{"unsupported_filter_field:" + filter.Field}
		}
		if filter.Op == "IS_NULL" || filter.Op == "is_null" || filter.Op == "IS_NOT_NULL" || filter.Op == "is_not_null" {
			op := " IS NULL"
			if filter.Op == "IS_NOT_NULL" || filter.Op == "is_not_null" {
				op = " IS NOT NULL"
			}
			where = append(where, "p."+column+op)
			continue
		}
		op := map[string]string{"eq": "=", "ne": "<>", "neq": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=", "=": "=", "!=": "<>", ">": ">", ">=": ">=", "<": "<", "<=": "<="}[filter.Op]
		if op == "" {
			return "", nil, tracePageUnsupported{"unsupported_filter_operator"}
		}
		if filter.Value == nil {
			if op != "=" && op != "<>" {
				where = append(where, "FALSE")
				continue
			}
			nullOp := " IS NULL"
			if op == "<>" {
				nullOp = " IS NOT NULL"
			}
			where = append(where, "p."+column+nullOp)
			continue
		}
		var value any
		if column == "name" || column == "model" || column == "trace_id" || column == "username" || column == "environment" {
			text, ok := filter.Value.(string)
			if !ok || !(op == "=" || op == "<>") {
				return "", nil, tracePageUnsupported{"unsupported_string_filter"}
			}
			value = text
		} else {
			text := fmt.Sprint(filter.Value)
			if column == "status" {
				if code, ok := map[string]string{"OK": "1", "ERROR": "2", "UNSET": "0"}[strings.ToUpper(text)]; ok {
					text = code
				}
			}
			number, err := strconv.ParseFloat(text, 64)
			if err != nil || math.IsNaN(number) || math.IsInf(number, 0) || math.Abs(number) > 9007199254740991 {
				return "", nil, tracePageUnsupported{"unsupported_numeric_precision"}
			}
			if column != "duration" && number != math.Trunc(number) {
				return "", nil, tracePageUnsupported{"unsupported_integer_operand"}
			}
			value = number
		}
		expr := "p." + column + op + "?"
		if column == "name" || column == "model" || column == "trace_id" || column == "username" || column == "environment" {
			expr = "BINARY p." + column + op + "BINARY ?"
		}
		if op == "<>" {
			expr = "(p." + column + " IS NULL OR " + expr + ")"
		}
		where = append(where, expr)
		args = append(args, value)
	}
	return strings.Join(where, " AND "), args, nil
}
func readModelOrder(order string) (string, error) {
	if order == "" {
		return "p.start_ns DESC,p.trace_id DESC", nil
	}
	direction := "ASC"
	field := order
	if strings.HasPrefix(field, "-") {
		direction = "DESC"
		field = strings.TrimPrefix(field, "-")
	}
	for _, suffix := range []string{".desc", ".asc", ":desc", ":asc"} {
		if strings.HasSuffix(field, suffix) {
			direction = strings.ToUpper(suffix[1:])
			field = strings.TrimSuffix(field, suffix)
		}
	}
	parts := strings.Fields(field)
	if len(parts) == 2 {
		field = parts[0]
		direction = strings.ToUpper(parts[1])
	}
	column := map[string]string{"start_time_unix_nano": "start_ns", "end_time_unix_nano": "end_ns", "period": "period", "duration": "duration", "duration_ms": "duration", "node_count": "node_count", "span_count": "node_count", "trace_id": "trace_id"}[field]
	if column == "" || (direction != "ASC" && direction != "DESC") {
		return "", tracePageUnsupported{"unsupported_order"}
	}
	return "(p." + column + " IS NULL) " + direction + ",p." + column + " " + direction + ",p.trace_id " + direction, nil
}

// readModelWindowParams preserves authorization and the frozen source window,
// while removing business predicates for the independent facets total contract.
func readModelWindowParams(p nativeListParams) (nativeListParams, bool) {
	filtered := p.Keyword != "" || p.InputKeyword != "" || len(p.Tags) > 0 || p.Env != "" || p.Scene != "" || p.Username != "" || len(p.Filters) > 0 || p.TraceID != "" || len(p.TraceIDs) > 0 || p.SessionID != "" || len(p.SessionIDs) > 0
	p.Keyword = ""
	p.InputKeyword = ""
	p.SearchType = ""
	p.Tags = nil
	p.Env = ""
	p.Scene = ""
	p.Username = ""
	p.Filters = nil
	p.TraceID = ""
	p.TraceIDs = nil
	p.SessionID = ""
	p.SessionIDs = nil
	return p, filtered
}

func (s *oceanBaseStore) readModelPage(ctx context.Context, org string, p nativeListParams) (any, error) {
	where, args, err := readModelWhere(org, p)
	if e, ok := err.(tracePageUnsupported); ok {
		return readModelUnsupported(e.reason), nil
	}
	if err != nil {
		return nil, err
	}
	order, err := readModelOrder(p.OrderBy)
	if e, ok := err.(tracePageUnsupported); ok {
		return readModelUnsupported(e.reason), nil
	}
	if err != nil {
		return nil, err
	}
	tx, err := s.beginReadModel(ctx, true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if reason, err := s.projectionMaintenanceReason(ctx, tx, false); err != nil {
		return nil, err
	} else if reason != "" {
		return readModelUnsupported(reason), nil
	}
	if ready, err := ingest.TraceProjectionSummaryColumnsReady(ctx, tx, s.cfg.TablePrefix); err != nil {
		return nil, err
	} else if !ready {
		return readModelUnsupported("projection_summary_columns_unavailable"), nil
	}
	for _, filter := range p.Filters {
		if filter.Field == "total_tokens" || filter.Field == "attributes.gen_ai.usage.total_tokens" {
			ready, reason, err := s.sessionTokenGeneratedCapability(ctx, tx)
			if err != nil {
				return nil, err
			}
			if !ready {
				return readModelUnsupported(reason), nil
			}
			break
		}
	}
	var covered bool
	err = tx.QueryRowContext(ctx, "SELECT complete FROM "+s.readModelTable("trace_projection_coverage")+" WHERE org_id=? AND space_id=?", org, p.SpaceID).Scan(&covered)
	if err == sql.ErrNoRows || err == nil && !covered {
		return readModelUnsupported("historical_coverage_incomplete"), nil
	}
	if err != nil {
		return nil, err
	}
	sources := s.readModelTable("trace_sources")
	projections := s.readModelTable("trace_projections")
	var stale, boundary, incompleteContent int64
	requireContent := readModelContentRequired(p)
	generatedContent, contentStrategy, err := s.contentCoverageCapability(ctx, tx, requireContent)
	if err != nil {
		return nil, err
	}
	coverageSQL := `SELECT COALESCE(SUM(CASE WHEN s.scope_valid=FALSE OR s.dirty=TRUE OR p.trace_id IS NULL OR p.source_revision<>s.source_revision OR p.semantics_version<>1 OR p.projection_context<>? THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN s.min_timestamp<? OR s.max_timestamp>=? THEN 1 ELSE 0 END),0),` + projectionIncompleteContentSQL(requireContent, generatedContent) + ` FROM ` + sources + ` s LEFT JOIN ` + projections + ` p ON p.org_id=s.org_id AND p.space_id=s.space_id AND p.trace_id=s.trace_id WHERE s.org_id=? AND s.space_id=? AND ((s.min_timestamp<? AND s.max_timestamp>=?) OR (p.period>=? AND p.period<?))`
	from, to := p.From*1000000, p.To*1000000
	coverageArgs := []any{p.ProjectionContext, from, to}
	if requireContent {
		coverageArgs = append(coverageArgs, p.AgentUUID)
	}
	coverageArgs = append(coverageArgs, org, p.SpaceID, to, from, p.From, p.To)
	// A correction can move a source to a new physical authorization scope
	// while the prior projection still belongs to the old scope. Coverage must
	// include both complete scope tuples before Agent matching; otherwise the
	// old caller can receive a dirty row, and the new caller a false empty page.
	if p.User != "" || p.AgentProduct != "" {
		currentScope, priorScope := []string{}, []string{"p.trace_id IS NOT NULL"}
		currentArgs, priorArgs := []any{}, []any{}
		if p.User != "" {
			currentScope = append(currentScope, "BINARY s.user_id=BINARY ?")
			priorScope = append(priorScope, "BINARY p.user_id=BINARY ?")
			currentArgs = append(currentArgs, p.User)
			priorArgs = append(priorArgs, p.User)
		}
		if p.AgentProduct != "" {
			currentScope = append(currentScope, "BINARY s.agent_product=BINARY ?")
			priorScope = append(priorScope, "BINARY p.agent_product=BINARY ?")
			currentArgs = append(currentArgs, p.AgentProduct)
			priorArgs = append(priorArgs, p.AgentProduct)
		}
		coverageSQL += " AND (s.scope_valid=FALSE OR (" + strings.Join(currentScope, " AND ") + ") OR (" + strings.Join(priorScope, " AND ") + "))"
		coverageArgs = append(coverageArgs, currentArgs...)
		coverageArgs = append(coverageArgs, priorArgs...)
	}
	if err := tx.QueryRowContext(ctx, coverageSQL, coverageArgs...).Scan(&stale, &boundary, &incompleteContent); err != nil {
		return nil, err
	}
	if boundary > 0 {
		return readModelUnsupported("boundary_crossing_trace"), nil
	}
	if stale > 0 {
		return readModelUnsupported("projection_coverage_stale"), nil
	}
	if incompleteContent > 0 {
		return readModelUnsupported("content_projection_incomplete"), nil
	}
	var total int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+projections+" p WHERE "+where, args...).Scan(&total); err != nil {
		return nil, err
	}
	windowTotal := total
	if windowParams, filtered := readModelWindowParams(p); filtered {
		windowWhere, windowArgs, err := readModelWhere(org, windowParams)
		if err != nil {
			return nil, err
		}
		// The same repeatable-read snapshot and typed summary table supply this
		// count. No canonical JSON or raw spans are scanned or decoded here.
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+projections+" p WHERE "+windowWhere, windowArgs...).Scan(&windowTotal); err != nil {
			return nil, err
		}
	}
	listArgs := append(append([]any{}, args...), p.Limit, (p.Page-1)*p.Limit)
	// Size only the selected page's stored summaries, before fetching/decoding.
	// LIMIT stays inside the aggregate so off-page rows cannot exhaust the page.
	var selectedBytes int64
	sizeQuery := "SELECT COALESCE(SUM(selected_bytes),0) FROM (SELECT OCTET_LENGTH(" + readModelStoredRowJSONSQL(p.MetadataOnly) + ")+OCTET_LENGTH(p.preview_json)+100 AS selected_bytes FROM " + projections + " p WHERE " + where + " ORDER BY " + order + " LIMIT ? OFFSET ?) selected_page"
	if err := tx.QueryRowContext(ctx, sizeQuery, listArgs...).Scan(&selectedBytes); err != nil {
		return nil, err
	}
	if selectedBytes < 0 || selectedBytes > projectionBatchPayloadLimit {
		return readModelUnsupported("projection_page_payload_limit"), nil
	}
	rows, err := scanReadModelRows(ctx, tx, "SELECT "+readModelStoredRowJSONSQL(p.MetadataOnly)+" AS row_json,p.preview_json,p.source_revision FROM "+projections+" p WHERE "+where+" ORDER BY "+order+" LIMIT ? OFFSET ?", listArgs...)
	if err != nil {
		return nil, err
	}
	data := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		canonical, err := tracePageJSON(row["row_json"])
		if err != nil {
			return readModelUnsupported("projection_row_invalid"), nil
		}
		preview, err := tracePageJSON(row["preview_json"])
		if err != nil {
			return readModelUnsupported("projection_preview_invalid"), nil
		}
		canonical["ioPreview"] = preview
		canonical["sourceRevision"] = row["source_revision"]
		data = append(data, canonical)
	}
	facets := nativeFacets{"env": {}, "scene": {}, "username": {}, "tags": {}}
	for _, f := range []struct {
		key, column string
		array       bool
	}{{"env", "environment", false}, {"username", "username", false}, {"scene", "scene_values_json", true}, {"tags", "tags_json", true}} {
		query := "SELECT p." + f.column + " AS value,COUNT(*) AS count FROM " + projections + " p WHERE " + where + " AND p." + f.column + "<>'' GROUP BY p." + f.column + " ORDER BY BINARY value LIMIT 500"
		if f.array {
			query = "SELECT j.value,COUNT(DISTINCT p.trace_id) AS count FROM " + projections + " p,JSON_TABLE(p." + f.column + ", '$[*]' COLUMNS (value VARCHAR(1024) PATH '$')) j WHERE " + where + " AND j.value<>'' GROUP BY BINARY j.value,j.value ORDER BY BINARY j.value LIMIT 500"
		}
		values, err := scanReadModelRows(ctx, tx, query, args...)
		if err != nil {
			return nil, err
		}
		if len(values) >= 500 {
			return readModelUnsupported("facet_cardinality_exceeded"), nil
		}
		for _, value := range values {
			count, countErr := asInt64(value["count"])
			if countErr != nil {
				return nil, fmt.Errorf("invalid facet count")
			}
			facets[f.key] = append(facets[f.key], nativeFacetValue{Value: value["value"], Count: int(count)})
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	meta := map[string]any{"contract": traceReadModelContract, "queryMode": "read_model", "queryContract": traceReadModelContract, "environmentSemantics": "root-span-resource-v1", "userIdentitySemantics": "trace-sdk-user-id-v1", "complete": true, "projection_context": p.ProjectionContext, "from": p.From, "to": p.To, "space_id": p.SpaceID, "agent_product": p.AgentProduct, "user": p.User, "agent_uuid": p.AgentUUID, "page": p.Page, "limit": p.Limit, "totalItems": total, "windowTotalItems": windowTotal, "totalPages": (total + p.Limit - 1) / p.Limit}
	meta["contentCoverageStrategy"] = contentStrategy
	result := map[string]any{"supported": true, "data": data, "facets": facets, "meta": meta}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	if len(encoded) > projectionBatchPayloadLimit {
		return readModelUnsupported("projection_page_payload_limit"), nil
	}
	return result, nil
}

// The shared durable state is read inside the request snapshot. A maintenance
// OFF transition must revoke this state before any untracked writes commit.
func (s *oceanBaseStore) projectionMaintenanceReason(ctx context.Context, tx *sql.Tx, lock bool) (string, error) {
	var enabled bool
	var err error
	if lock {
		enabled, _, err = ingest.LockProjectionMaintenance(ctx, tx, s.cfg.TablePrefix)
	} else {
		enabled, _, err = ingest.ReadProjectionMaintenance(ctx, tx, s.cfg.TablePrefix)
	}
	if err != nil {
		return "", err
	}
	if !enabled {
		return "projection_maintenance_unavailable", nil
	}
	return "", nil
}

// Resume changes a database-wide generation, so organization administrator
// permission alone is insufficient. The route also requires AdminAccess and
// this existing deployment-operator ingestion secret. Ordinary projectors do
// not possess or need that secret.
func projectionResumeOperatorAuthorized(r *http.Request) bool {
	expected := os.Getenv("SIGNOZ_INGEST_INTERNAL_KEY")
	supplied := r.Header.Get("X-Signoz-Ingest-Key")
	return expected != "" && supplied != "" && subtle.ConstantTimeCompare([]byte(expected), []byte(supplied)) == 1
}
