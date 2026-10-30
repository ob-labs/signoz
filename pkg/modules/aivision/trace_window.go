package aivision

// Complete-window recovery uses a single repeatable-read transaction. It joins
// trustworthy summaries with raw spans which are dirty, cross the boundary, or
// cannot be projected. It never performs one raw-detail request per Trace.
import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/SigNoz/signoz/pkg/ingest"
)

const traceWindowContract = "native-trace-window-v1"
const traceWindowTraceLimit = 50000
const traceWindowSpanLimit = 500000
const traceWindowPayloadLimit = 24 * 1024 * 1024

// Source admission failures must not trigger the representation-only raw retry.
var errTraceWindowResourceBudget = errors.New("complete recovery window exceeds resource bound")

func (h *handler) traceWindowParams(r *http.Request) (nativeListParams, bool, error) {
	allowed := map[string]bool{}
	for _, key := range []string{"from", "to", "space_id", "user", "agent_product", "agent_uuid", "projection_context", "raw"} {
		allowed[key] = true
	}
	for key, values := range r.URL.Query() {
		if !allowed[key] || len(values) != 1 {
			return nativeListParams{}, false, fmt.Errorf("unsupported or repeated window parameter: %s", key)
		}
	}
	p, err := h.nativeParams(r)
	if err != nil {
		return p, false, err
	}
	// Recovery preserves the caller's exact physical authorization tuple.
	// The older native parser normalizes whitespace for legacy endpoints.
	p.User, p.SpaceID, p.AgentProduct = r.URL.Query().Get("user"), r.URL.Query().Get("space_id"), r.URL.Query().Get("agent_product")
	p.AgentUUID = r.URL.Query().Get("agent_uuid")
	p.ProjectionContext = r.URL.Query().Get("projection_context")
	raw := r.URL.Query().Get("raw")
	if raw != "true" && raw != "false" {
		return p, false, fmt.Errorf("raw must be true or false")
	}
	if len(p.AgentUUID) > 255 || !projectionContextPattern.MatchString(p.ProjectionContext) {
		return p, false, fmt.Errorf("invalid Agent identity or projection context")
	}
	if _, _, err = traceWindowScope("org", p, ""); err != nil {
		return p, false, err
	}
	return p, raw == "true", nil
}

func traceWindowScope(org string, p nativeListParams, alias string) (string, []any, error) {
	if _, _, err := nativeTraceWhere(org, nativeListParams{From: p.From, To: p.To, SpaceID: p.SpaceID, User: p.User, AgentProduct: p.AgentProduct}); err != nil {
		return "", nil, err
	}
	columns := []string{"org_id", "space_id"}
	values := []any{org, p.SpaceID}
	if p.User != "" {
		columns = append(columns, "user_id")
		values = append(values, p.User)
	}
	if p.AgentProduct != "" {
		columns = append(columns, "agent_product")
		values = append(values, p.AgentProduct)
	}
	clauses := []string{}
	for _, column := range columns {
		clauses = append(clauses, alias+column+"=? AND BINARY "+alias+column+"=BINARY ?")
	}
	clauses = append(clauses, alias+"timestamp>=?", alias+"timestamp<?")
	expanded := make([]any, 0, len(values)*2+2)
	for _, value := range values {
		expanded = append(expanded, value, value)
	}
	values = append(expanded, p.From*1000000, p.To*1000000)
	return strings.Join(clauses, " AND "), values, nil
}

// This predicate deliberately checks current physical scope, previous canonical
// scope and revision before trusting any contents. Other Agents are skipped only
// after this proof; ambiguous raw resources are validated by the canonical owner.
func (s *oceanBaseStore) windowCleanRelation(org string, p nativeListParams) (string, []any) {
	predicate := `s.org_id=? AND BINARY s.org_id=BINARY ? AND s.space_id=? AND BINARY s.space_id=BINARY ? AND s.trace_id=r.trace_id AND BINARY s.trace_id=BINARY r.trace_id AND s.scope_valid=TRUE AND s.dirty=FALSE AND p.source_revision=s.source_revision AND p.semantics_version=1 AND p.projection_context=? AND s.min_timestamp>=? AND s.max_timestamp<? AND BINARY p.user_id=BINARY s.user_id AND BINARY p.agent_product=BINARY s.agent_product`
	args := []any{org, org, p.SpaceID, p.SpaceID, p.ProjectionContext, p.From * 1000000, p.To * 1000000}
	if p.User != "" {
		predicate += " AND BINARY s.user_id=BINARY ?"
		args = append(args, p.User)
	}
	if p.AgentProduct != "" {
		predicate += " AND BINARY s.agent_product=BINARY ?"
		args = append(args, p.AgentProduct)
	}
	relation := s.readModelTable("trace_sources") + " s JOIN " + s.readModelTable("trace_projections") + " p ON p.org_id=s.org_id AND p.space_id=s.space_id AND p.trace_id=s.trace_id AND BINARY p.org_id=BINARY s.org_id AND BINARY p.space_id=BINARY s.space_id AND BINARY p.trace_id=BINARY s.trace_id"
	return relation + " WHERE " + predicate, args
}

func (s *oceanBaseStore) traceWindow(ctx context.Context, org string, p nativeListParams, rawOnly bool) (any, error) {
	return s.traceWindowWithPayloadLimit(ctx, org, p, rawOnly, traceWindowPayloadLimit)
}

func (s *oceanBaseStore) traceWindowWithPayloadLimit(ctx context.Context, org string, p nativeListParams, rawOnly bool, payloadLimit int) (any, error) {
	return s.traceWindowRepresentation(ctx, org, p, rawOnly, payloadLimit, false)
}

func (s *oceanBaseStore) traceMetricsWindow(ctx context.Context, org string, p nativeListParams, rawOnly bool) (any, error) {
	return s.traceWindowRepresentation(ctx, org, p, rawOnly, traceWindowPayloadLimit, true)
}

func (s *oceanBaseStore) traceWindowRepresentation(ctx context.Context, org string, p nativeListParams, rawOnly bool, payloadLimit int, metricsOnly bool) (any, error) {
	scope, args, err := traceWindowScope(org, p, "")
	if err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	useProjection := false
	var projectionGeneration int64
	if !rawOnly {
		useProjection, projectionGeneration, err = ingest.ReadProjectionMaintenance(ctx, tx, s.cfg.TablePrefix)
		if err != nil {
			return nil, err
		}
	}
	var traceCount, spanCount, payloadBytes int64
	// Bound the transfer before loading LONGTEXT values into either service.
	var maximumSpanBytes int64
	sizeSQL := "SELECT COUNT(DISTINCT BINARY trace_id),COUNT(*),COALESCE(SUM(" + nativeSpanSourceBytesSQL() + "),0),COALESCE(MAX(" + nativeSpanSourceBytesSQL() + "),0) FROM " + s.tracesTable() + " WHERE " + scope
	if err = tx.QueryRowContext(ctx, sizeSQL, args...).Scan(&traceCount, &spanCount, &payloadBytes, &maximumSpanBytes); err != nil {
		return nil, err
	}
	if traceCount < 0 || spanCount < 0 || payloadBytes < 0 {
		return nil, fmt.Errorf("invalid recovery window counts")
	}
	if traceCount > traceWindowTraceLimit || spanCount > traceWindowSpanLimit || payloadBytes > traceWindowSourceByteLimit || maximumSpanBytes > traceSpanSourceByteLimit {
		return nil, fmt.Errorf("%w (50000 Traces,500000 spans,512 MiB source,16 MiB per span); narrow the time range", errTraceWindowResourceBudget)
	}
	canonical := []map[string]any{}
	cleanCount := int64(0)
	selectedSpanCount := int64(0)
	wireBudget := newTraceWindowWireBudget(payloadLimit)
	rawScope, rawArgs, err := traceWindowScope(org, p, "r.")
	if err != nil {
		return nil, err
	}
	fullRawScope, fullRawArgs := rawScope, rawArgs
	cleanRelation, cleanArgs := s.windowCleanRelation(org, p)
	recoveryReason := ""
	// A denormalized canonical row may be larger than its compact raw source.
	// Discard only the attempted representation, never the storage snapshot or
	// its resource/auth scope. A raw retry still passes every original bound.
	resetToRaw := func() {
		canonical = []map[string]any{}
		cleanCount, selectedSpanCount = 0, 0
		wireBudget = newTraceWindowWireBudget(payloadLimit)
		rawScope, rawArgs = fullRawScope, fullRawArgs
		useProjection = false
		recoveryReason = "canonical_summary_budget"
	}
	candidate := "SELECT trace_id,COUNT(*) AS span_count FROM " + s.tracesTable() + " WHERE " + scope + " GROUP BY BINARY trace_id,trace_id"
	storedMetrics := false
	if useProjection && metricsOnly {
		storedMetrics, err = ingest.TraceProjectionMetricsWindowColumnsReady(ctx, tx, s.cfg.TablePrefix)
		if err != nil {
			return nil, err
		}
		// Older/conflicting schemas retain the existing bounded stream. A
		// missing optimization must not turn compact metrics into a raw-only
		// response that exceeds its wire budget.
	}
	if useProjection && metricsOnly {
		// Compact metrics may be small even when full canonical IO is large.
		// Bound one historical/direct-writer row before decoding, not the sum
		// of IO across the window. Ordinary publications are limited to 16 MiB.
		var maximumProjectionBytes int64
		query := "SELECT COALESCE(MAX(OCTET_LENGTH(p.row_json)),0) FROM (" + candidate + ") r JOIN " + cleanRelation
		values := append(append([]any{}, args...), cleanArgs...)
		if err := tx.QueryRowContext(ctx, query, values...).Scan(&maximumProjectionBytes); err != nil {
			return nil, err
		}
		if maximumProjectionBytes < 0 {
			return nil, fmt.Errorf("invalid canonical recovery row byte count")
		}
		if maximumProjectionBytes > sourcePageTraceByteLimit {
			resetToRaw()
		}
	}
	if useProjection && !metricsOnly {
		// Canonical rows duplicate IO and may exceed the compact raw form. This
		// conservative estimate avoids loading a known-large representation.
		// It includes all clean physical candidates which the row query carries;
		// exact escaping/framing is still enforced while collecting either form.
		var summaryBytes int64
		query := "SELECT COALESCE(SUM(OCTET_LENGTH(p.row_json)+OCTET_LENGTH(p.preview_json)),0) FROM (" + candidate + ") r JOIN " + cleanRelation
		values := append(append([]any{}, args...), cleanArgs...)
		if err := tx.QueryRowContext(ctx, query, values...).Scan(&summaryBytes); err != nil {
			return nil, err
		}
		if summaryBytes < 0 {
			return nil, fmt.Errorf("invalid canonical recovery byte count")
		}
		if summaryBytes > int64(payloadLimit-wireBudget.used) {
			resetToRaw()
		}
	}
	if useProjection {
		// r is the distinct raw candidate set, preventing a stale/orphan projection
		// from inventing rows when source reconciliation is still catching up.
		columns := "r.trace_id,r.span_count,p.agent_uuid,p.row_json,p.preview_json,p.source_revision"
		if metricsOnly {
			columns = "r.trace_id,r.span_count,p.agent_uuid,p.row_json,p.source_revision"
		}
		if metricsOnly && storedMetrics {
			// Never transfer or parse full canonical IO on a metrics read. Stored
			// helpers are backfilled and maintained even by older publishers.
			columns = "r.trace_id,r.span_count,p.agent_uuid,p." + ingest.TraceProjectionMetadataColumn + " AS row_json,p." + ingest.TraceProjectionRowKeysColumn + " AS row_keys,p." + ingest.TraceProjectionMetricsColumn + " AS metrics,p." + ingest.TraceProjectionOperationPreviewsColumn + " AS operation_previews,p.source_revision"
		}
		query := "SELECT " + columns + " FROM (" + candidate + ") r JOIN " + cleanRelation
		values := append(append([]any{}, args...), cleanArgs...)
		seen := map[string]bool{}
		err = scanReadModelRowsEach(ctx, tx, query, values, func(record map[string]any) error {
			id, ok := record["trace_id"].(string)
			if !ok || id == "" || seen[id] {
				return fmt.Errorf("duplicate canonical recovery Trace")
			}
			seen[id] = true
			count, e := asInt64(record["span_count"])
			if e != nil || count < 1 {
				return fmt.Errorf("invalid canonical recovery span count")
			}
			selectedSpanCount += count
			cleanCount++
			if p.AgentUUID != "" && record["agent_uuid"] != p.AgentUUID {
				return nil
			}
			row, e := tracePageJSON(record["row_json"])
			if e != nil || row["trace_id"] != id {
				return fmt.Errorf("invalid canonical recovery row")
			}
			if metricsOnly {
				if storedMetrics {
					row, e = traceMetricsWindowStoredRow(row, record)
					if e != nil {
						return e
					}
				} else {
					row = traceMetricsWindowRow(row)
				}
			} else {
				preview, e := tracePageJSON(record["preview_json"])
				if e != nil {
					return fmt.Errorf("invalid canonical recovery preview")
				}
				row["ioPreview"] = preview
			}
			row["sourceRevision"] = record["source_revision"]
			if err := wireBudget.addRow(row); err != nil {
				return err
			}
			canonical = append(canonical, row)
			return nil
		})
		if errors.Is(err, errTraceWindowPayloadBudget) {
			resetToRaw()
		} else if err != nil {
			return nil, err
		}
		if useProjection {
			rawScope += " AND NOT EXISTS (SELECT 1 FROM " + cleanRelation + ")"
			rawArgs = append(rawArgs, cleanArgs...)
		}
	}

	details := []map[string]any{}
	rawSpanCount := int64(0)
	readRaw := func() error {
		details = []map[string]any{}
		seenSpans := map[string]bool{}
		lastID := ""
		var current []map[string]any
		rawSpanCount = 0
		err := scanReadModelRowsEach(ctx, tx, "SELECT "+nativeSpanProjection+" FROM "+s.tracesTable()+" r WHERE "+rawScope+" ORDER BY BINARY trace_id,timestamp,BINARY span_id", rawArgs, func(row map[string]any) error {
			id, ok := row["trace_id"].(string)
			sid, sok := row["span_id"].(string)
			if !ok || !sok || id == "" || sid == "" || seenSpans[id+"/"+sid] {
				return fmt.Errorf("duplicate or missing raw recovery identity")
			}
			seenSpans[id+"/"+sid] = true
			if id != lastID {
				if err := wireBudget.addRawGroup(id); err != nil {
					return err
				}
				if lastID != "" {
					details = append(details, map[string]any{"traceId": lastID, "spans": current})
				}
				lastID, current = id, []map[string]any{}
			}
			if err := decodeWindowSpan(row); err != nil {
				return err
			}
			if err := compactNativeSpan(row); err != nil {
				return err
			}
			if err := wireBudget.addRow(row); err != nil {
				return err
			}
			rawSpanCount++
			current = append(current, row)
			return nil
		})
		if err != nil {
			return err
		}
		if lastID != "" {
			details = append(details, map[string]any{"traceId": lastID, "spans": current})
		}
		return nil
	}
	err = readRaw()
	if errors.Is(err, errTraceWindowPayloadBudget) && useProjection {
		resetToRaw()
		err = readRaw()
	}
	if err != nil {
		return nil, err
	}
	if int64(len(details))+cleanCount != traceCount || rawSpanCount+selectedSpanCount != spanCount {
		return nil, fmt.Errorf("incomplete canonical recovery window")
	}
	meta := map[string]any{"from": p.From, "to": p.To, "space_id": p.SpaceID, "user": p.User, "agent_product": p.AgentProduct, "agent_uuid": p.AgentUUID, "projection_context": p.ProjectionContext, "raw": !useProjection, "traceCount": traceCount, "spanCount": spanCount, "rawSpanCount": rawSpanCount, "projectedTraceCount": cleanCount, "rawTraceCount": len(details), "omittedAgentTraceCount": cleanCount - int64(len(canonical))}
	// Only external hydration of trusted summaries needs an epoch proof. Raw
	// recovery remains independent of missing or unavailable derived controls.
	if useProjection && len(canonical) > 0 {
		if projectionGeneration < 1 {
			return nil, fmt.Errorf("invalid recovery projection generation")
		}
		meta["maintenance_generation"] = projectionGeneration
	}
	if metricsOnly {
		meta["representation"] = "metrics-v1"
	}
	if recoveryReason != "" {
		meta["recoveryReason"] = recoveryReason
	}
	if err := wireBudget.validateEnvelope(meta); err != nil {
		return nil, err
	}
	result := traceWindowResponse(meta, canonical, details)
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func decodeWindowSpan(row map[string]any) error {
	for _, column := range []string{"resource_attributes", "attributes", "events", "links", "payload"} {
		if raw, ok := row[column].(string); ok {
			d := json.NewDecoder(strings.NewReader(raw))
			d.UseNumber()
			var value, extra any
			if d.Decode(&value) != nil || d.Decode(&extra) != io.EOF {
				return fmt.Errorf("invalid raw recovery JSON: %s", column)
			}
			row[column] = value
		}
	}
	row["resource"] = row["resource_attributes"]
	normalizeNativeSpan(row)
	return nil
}

// Stored metadata uses JSON_OBJECT, which adds nulls for absent keys. Recover
// exactly the old whitelist projection using persisted original key presence.
// Keep complete observations/previews, including malformed types, so canonical
// validation sees the same facts as it did when reading row_json directly.
func traceMetricsWindowStoredRow(metadata, record map[string]any) (map[string]any, error) {
	decode := func(value any) (any, error) {
		if value == nil {
			return nil, nil
		}
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("invalid stored metrics JSON")
		}
		decoder := json.NewDecoder(strings.NewReader(text))
		decoder.UseNumber()
		var result any
		if err := decoder.Decode(&result); err != nil {
			return nil, err
		}
		var trailing any
		if decoder.Decode(&trailing) != io.EOF {
			return nil, fmt.Errorf("invalid trailing stored metrics JSON")
		}
		return result, nil
	}
	value, err := decode(record["row_keys"])
	if err != nil {
		return nil, err
	}
	keys, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("invalid stored metrics row keys")
	}
	present := make(map[string]bool, len(keys))
	for _, value := range keys {
		key, ok := value.(string)
		if !ok || present[key] {
			return nil, fmt.Errorf("invalid stored metrics row key")
		}
		present[key] = true
	}
	row := make(map[string]any, len(traceMetricsWindowFields))
	for _, field := range traceMetricsWindowFields {
		if !present[field] {
			continue
		}
		switch field {
		case "_metrics", "_operation_previews":
			column := "metrics"
			if field == "_operation_previews" {
				column = "operation_previews"
			}
			row[field], err = decode(record[column])
			if err != nil {
				return nil, err
			}
		default:
			value, ok := metadata[field]
			if !ok {
				return nil, fmt.Errorf("incomplete stored metrics metadata")
			}
			row[field] = value
		}
	}
	return row, nil
}
