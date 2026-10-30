package aivision

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/SigNoz/signoz/pkg/ingest"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

const nativeTracePageContract = "native-trace-page-v1"

// Resource preflight and page data share a repeatable-read snapshot. Metadata
// is stored separately from raw payloads; only current-page spans are loaded.
func (h *handler) TracePage(w http.ResponseWriter, r *http.Request) {
	org, ok := h.organization(w, r)
	if !ok {
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	allowed := map[string]bool{}
	for _, key := range []string{"from", "to", "user", "space_id", "agent_product", "agent_uuid", "trace_id", "trace_ids", "session_id", "session_ids", "fields", "filters", "orderBy", "order_by", "page", "limit", "searchType", "search_type", "keyword", "input_keyword", "tags", "env", "scene", "username"} {
		allowed[key] = true
	}
	for key := range r.URL.Query() {
		if !allowed[key] {
			writeJSON(w, http.StatusOK, map[string]any{"supported": false, "reason": "unsupported trace page parameter: " + key})
			return
		}
	}
	p, err := h.nativeParams(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	query := r.URL.Query()
	p.AgentUUID = query.Get("agent_uuid")
	p.TraceID, p.Keyword = query.Get("trace_id"), query.Get("keyword")
	p.Env, p.Scene, p.Username = query.Get("env"), query.Get("scene"), query.Get("username")
	// The ordinary native endpoints trim filter values and decode numbers as
	// float64. This contract preserves canonical exact string/int64 semantics.
	if raw := query.Get("filters"); raw != "" {
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&p.Filters); err != nil {
			writeAPIError(w, http.StatusBadRequest, "bad_data", "filters must be a JSON array")
			return
		}
	}
	if raw, exists := query["tags"]; exists {
		p.Tags = raw
		if len(raw) == 1 && strings.HasPrefix(strings.TrimSpace(raw[0]), "[") {
			if err := json.Unmarshal([]byte(raw[0]), &p.Tags); err != nil {
				writeAPIError(w, http.StatusBadRequest, "bad_data", "tags must be a JSON string array")
				return
			}
		}
	}
	p.Limit = 50
	if raw := query.Get("limit"); raw != "" {
		p.Limit, _ = strconv.Atoi(raw)
	}
	if p.Limit < 1 || p.Limit > 200 || len(p.AgentUUID) > 255 {
		writeAPIError(w, http.StatusBadRequest, "bad_data", "limit must be between 1 and 200 and agent_uuid at most 255 characters")
		return
	}
	if !acquireTraceQuery(w) {
		return
	}
	defer releaseTraceQuery()
	ctx, cancel := context.WithTimeout(r.Context(), h.cfg.QueryTimeout)
	defer cancel()
	result, err := h.backend.tracePage(ctx, org, p)
	if err != nil {
		if errors.Is(err, errTracePageBudget) {
			writeTraceCapacityError(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		h.writeBackendError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type tracePageUnsupported struct{ reason string }

func (e tracePageUnsupported) Error() string { return e.reason }

type tracePageItem struct {
	TraceID   string           `json:"traceId"`
	SpanCount int              `json:"spanCount"`
	Spans     []map[string]any `json:"spans"`
}

func (store *oceanBaseStore) tracePage(ctx context.Context, org string, p nativeListParams) (any, error) {
	query, args, err := buildTracePageSQL(store.tracesTable(), org, p, store.cfg.MaxLimit)
	if unsupported, ok := err.(tracePageUnsupported); ok {
		return map[string]any{"supported": false, "reason": unsupported.reason}, nil
	}
	if err != nil {
		return nil, err
	}
	if store.db == nil {
		return nil, fmt.Errorf("trace page requires a snapshot-capable database")
	}
	tx, err := store.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	ready, err := ingest.TracePageDimensionsReady(ctx, tx, store.cfg.TablePrefix)
	if err != nil {
		return nil, err
	}
	if !ready {
		return nil, fmt.Errorf("trace page requires completed metadata migration")
	}
	physical := nativeListParams{From: p.From, To: p.To, User: p.User, SpaceID: p.SpaceID, AgentProduct: p.AgentProduct}
	scope, scopeArgs, err := nativeTraceWhere(org, physical)
	if err != nil {
		return nil, err
	}
	var count, total, maximum int64
	sizeSQL := "SELECT COUNT(*),COALESCE(SUM(OCTET_LENGTH(" + ingest.TracePageDimensionsColumn + ")),0),COALESCE(MAX(OCTET_LENGTH(" + ingest.TracePageDimensionsColumn + ")),0) FROM " + store.tracesTable() + " WHERE " + scope
	if err = tx.QueryRowContext(ctx, sizeSQL, scopeArgs...).Scan(&count, &total, &maximum); err != nil {
		return nil, err
	}
	if count > 200000 || total > 16*1024*1024 || maximum > 64*1024 {
		return nil, fmt.Errorf("%w: metadata window exceeds 200000 spans, 16 MiB metadata or 64 KiB per span; narrow the time range", errTracePageBudget)
	}
	// The exact page relation is reused for preflight, before JSON aggregation.
	// Six is the maximum JSON escaping expansion for an arbitrary UTF-8 byte.
	common := strings.Split(query, "\nSELECT CAST(JSON_ARRAYAGG")[0]
	preflight := common + " SELECT COALESCE((SELECT SUM(source_bytes*6+512) FROM base b JOIN page p ON BINARY b.trace_id=BINARY p.trace_id),0)+COALESCE((SELECT SUM(OCTET_LENGTH(value)*6+256) FROM facet_counts),0)+4096"
	var wireBound int64
	if err = tx.QueryRowContext(ctx, preflight, args[:len(args)-len(scopeArgs)-2]...).Scan(&wireBound); err != nil {
		return nil, err
	}
	if wireBound > 24*1024*1024 {
		return nil, fmt.Errorf("%w: page and facets exceed 24 MiB encoded budget; reduce page size or narrow the time range", errTracePageBudget)
	}
	rows, err := scanReadModelRows(ctx, tx, query, args...)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, fmt.Errorf("trace page envelope is missing")
	}
	raw, ok := rows[0]["trace_page_rows"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid trace page envelope")
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var envelope []map[string]any
	var trailing any
	if decoder.Decode(&envelope) != nil || decoder.Decode(&trailing) != io.EOF {
		return nil, fmt.Errorf("invalid trace page envelope JSON")
	}
	// JSON_ARRAYAGG does not promise element order. Rank is calculated inside
	// the same SQL snapshot; sorting at most limit+5 envelopes is bounded.
	sort.Slice(envelope, func(i, j int) bool {
		si, _ := asInt64(envelope[i]["section"])
		sj, _ := asInt64(envelope[j]["section"])
		if si != sj {
			return si < sj
		}
		pi, _ := asInt64(envelope[i]["pageRank"])
		pj, _ := asInt64(envelope[j]["pageRank"])
		return pi < pj
	})
	return decodeTracePage(envelope, p)
}

// Values in the projection are JSON, rather than driver-dependent SQL numeric
// types. UseNumber keeps nanosecond timestamps and OTLP int64 attributes exact.
func tracePageJSON(value any) (map[string]any, error) {
	if result, ok := value.(map[string]any); ok {
		return result, nil
	}
	var source string
	switch value := value.(type) {
	case string:
		source = value
	case []byte:
		source = string(value)
	default:
		return nil, fmt.Errorf("invalid trace page JSON result")
	}
	decoder := json.NewDecoder(strings.NewReader(source))
	decoder.UseNumber()
	var result map[string]any
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return nil, fmt.Errorf("invalid trailing trace page JSON")
	}
	return result, nil
}

// Completeness proofs are integer counts, unlike timestamps where the legacy
// decoder deliberately accepts fractional/exponent forms and truncates them.
func tracePageCount(value any) (int64, error) {
	var count int64
	switch value := value.(type) {
	case int:
		count = int64(value)
	case int64:
		count = value
	case uint64:
		if value > math.MaxInt64 {
			return 0, fmt.Errorf("trace page count overflows int64")
		}
		count = int64(value)
	case json.Number:
		var err error
		count, err = value.Int64()
		if err != nil {
			return 0, fmt.Errorf("trace page count must be an integer")
		}
	default:
		return 0, fmt.Errorf("trace page count must be an integer")
	}
	if count < 0 {
		return 0, fmt.Errorf("trace page count must be non-negative")
	}
	return count, nil
}

func decodeTracePage(rows []map[string]any, p nativeListParams) (any, error) {
	data := make([]tracePageItem, 0)
	indices := map[string]int{}
	facets := nativeFacets{"env": {}, "scene": {}, "username": {}, "tags": {}}
	metaSeen, total := false, int64(0)
	for _, row := range rows {
		v, err := tracePageJSON(row["result"])
		if err != nil {
			return nil, err
		}
		switch fmt.Sprint(row["kind"]) {
		case "meta":
			if metaSeen {
				return nil, fmt.Errorf("duplicate trace page metadata")
			}
			metaSeen = true
			invalid, err := tracePageCount(v["invalidScope"])
			if err != nil || invalid != 0 {
				return nil, fmt.Errorf("trace page contains invalid span identity or duplicate span IDs")
			}
			oversized, err := tracePageCount(v["oversizedTraces"])
			if err != nil || oversized != 0 {
				return nil, fmt.Errorf("trace page exceeds the per-trace span limit; narrow the time range")
			}
			unsupported, err := tracePageCount(v["unsupportedValues"])
			if err != nil {
				return nil, err
			}
			if unsupported != 0 {
				return map[string]any{"supported": false, "reason": "trace dimension or tag values require canonical projection"}, nil
			}
			total, err = tracePageCount(v["totalItems"])
			if err != nil || total < 0 {
				return nil, fmt.Errorf("invalid trace page total")
			}
		case "facet":
			dimension, ok := v["dimension"].(string)
			if !ok {
				return nil, fmt.Errorf("invalid trace page facet dimension")
			}
			if _, ok := facets[dimension]; !ok {
				return nil, fmt.Errorf("unknown trace page facet dimension")
			}
			values, ok := v["values"].([]any)
			if !ok {
				return nil, fmt.Errorf("invalid trace page facet values")
			}
			for _, raw := range values {
				item, ok := raw.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("invalid trace page facet")
				}
				value, ok := item["value"].(string)
				count, err := tracePageCount(item["count"])
				if !ok || err != nil || count < 1 {
					return nil, fmt.Errorf("invalid trace page facet")
				}
				facets[dimension] = append(facets[dimension], nativeFacetValue{Value: value, Count: int(count)})
			}
		case "trace":
			id, ok := v["traceId"].(string)
			count, err := tracePageCount(v["spanCount"])
			if _, exists := indices[id]; exists || !ok || id == "" || err != nil || count < 1 {
				return nil, fmt.Errorf("invalid trace page item")
			}
			indices[id] = len(data)
			spans, ok := v["spans"].([]any)
			if !ok {
				return nil, fmt.Errorf("trace page spans are missing")
			}
			item := tracePageItem{TraceID: id, SpanCount: int(count), Spans: make([]map[string]any, 0, len(spans))}
			for _, raw := range spans {
				span, ok := raw.(map[string]any)
				if !ok || span["trace_id"] != id {
					return nil, fmt.Errorf("trace page span has no selected trace")
				}
				// Keep OTLP JSON as raw TEXT inside the SQL envelope: parsing it
				// in the database could round arbitrary JSON numeric values.
				for _, key := range []string{"attributes", "resource_attributes", "events", "links", "payload"} {
					if raw, ok := span[key].(string); ok {
						decoder := json.NewDecoder(strings.NewReader(raw))
						decoder.UseNumber()
						var value, trailing any
						if decoder.Decode(&value) == nil && decoder.Decode(&trailing) == io.EOF {
							span[key] = value
						}
					}
				}
				span["resource"] = span["resource_attributes"]
				normalizeNativeSpan(span)
				item.Spans = append(item.Spans, span)
			}
			data = append(data, item)
		default:
			return nil, fmt.Errorf("unknown trace page row kind")
		}
	}
	if !metaSeen {
		return nil, fmt.Errorf("trace page metadata is missing")
	}
	for _, item := range data {
		ids := map[string]bool{}
		for _, span := range item.Spans {
			id, ok := span["span_id"].(string)
			if !ok || id == "" || ids[id] {
				return nil, fmt.Errorf("duplicate or invalid trace page span ID")
			}
			ids[id] = true
		}
		if len(ids) != item.SpanCount {
			return nil, fmt.Errorf("incomplete trace page spans")
		}
	}
	expected := int64(p.Limit)
	if remaining := total - int64(p.Page-1)*int64(p.Limit); remaining < expected {
		expected = max(remaining, 0)
	}
	if int64(len(data)) != expected {
		return nil, fmt.Errorf("incomplete trace page")
	}
	for _, values := range facets {
		sort.Slice(values, func(i, j int) bool { return values[i].Value.(string) < values[j].Value.(string) })
	}
	return map[string]any{
		"data": data, "facets": facets,
		"meta": map[string]any{"contract": nativeTracePageContract, "complete": true,
			"page": p.Page, "limit": p.Limit, "totalItems": total, "totalPages": (total + int64(p.Limit) - 1) / int64(p.Limit),
			"from": p.From, "to": p.To, "space_id": p.SpaceID, "agent_uuid": p.AgentUUID, "agent_product": p.AgentProduct, "user": p.User},
	}, nil
}

func pageJSONKey(column, key string) string { return `JSON_EXTRACT(` + column + `, '$."` + key + `"')` }
func pageHasKey(column, key string) string {
	return `JSON_CONTAINS_PATH(` + column + `, 'one', '$."` + key + `"')`
}
func pageText(expression string) string {
	return "CASE WHEN JSON_TYPE(" + expression + ") = 'NULL' THEN NULL ELSE JSON_UNQUOTE(" + expression + ") END"
}
func pageAttribute(key string) string {
	return "CASE WHEN " + pageHasKey("attributes", key) + " THEN " + pageJSONKey("attributes", key) + " ELSE " + pageJSONKey("JSON_EXTRACT(payload, '$.attributes')", key) + " END"
}
func pageHasAttribute(key string) string {
	return "(" + pageHasKey("attributes", key) + " OR " + pageHasKey("JSON_EXTRACT(payload, '$.attributes')", key) + ")"
}

var tracePageColumns = map[string]string{
	"trace_id": "s.trace_id", "start_time_unix_nano": "s.start_ns", "end_time_unix_nano": "s.end_ns",
	"duration_ms": "s.duration_ms", "status_code": "s.status_code", "span_count": "s.span_count",
}
var tracePageDimensions = map[string]string{
	"resource.attributes.ant.agent.env": "env", "env": "env",
	"resource.attributes.ant.agent.scene": "scene", "scene": "scene",
	"attributes.ant.username": "username", "username": "username",
	"resource.attributes.aivision.agent.id": "agent_uuid",
}

func tracePageOrder(raw string) (string, error) {
	field, direction := "start_time_unix_nano", "DESC"
	if raw != "" {
		field, direction = strings.TrimSpace(raw), "ASC"
		if strings.HasPrefix(field, "-") {
			field, direction = strings.TrimPrefix(field, "-"), "DESC"
		}
		if strings.HasSuffix(field, ".desc") {
			field, direction = strings.TrimSuffix(field, ".desc"), "DESC"
		}
		field = strings.TrimSuffix(field, ".asc")
	}
	column, ok := tracePageColumns[field]
	if !ok {
		return "", tracePageUnsupported{"unsupported trace page sort: " + field}
	}
	if field == "trace_id" {
		column = "BINARY " + column
	}
	// Python's canonical ordering sorts (is_null, value, trace_id), reversing
	// all three components for DESC; SQL's default NULL order is the opposite.
	return "(" + column + " IS NULL) " + direction + ", " + column + " " + direction + ", BINARY s.trace_id " + direction, nil
}

func tracePageCompare(expression, field string, f nativeFilter) (string, []any, error) {
	op := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(f.Op), "_", " "))
	if op == "" || op == "EQ" {
		op = "="
	}
	if op == "NEQ" {
		op = "!="
	}
	if op == "IS NULL" || op == "IS NOT NULL" {
		return expression + " " + op, nil, nil
	}
	numeric := field != "trace_id" && tracePageColumns[field] != ""
	convert := func(v any) (any, error) {
		if v == nil {
			return nil, nil
		}
		if numeric {
			if field == "status_code" {
				if name, ok := v.(string); ok {
					if code, ok := map[string]int{"OK": 1, "ERROR": 2, "UNSET": 0}[strings.ToUpper(name)]; ok {
						v = code
					}
				}
			}
			if _, ok := v.(bool); ok {
				return nil, fmt.Errorf("invalid numeric trace filter")
			}
			if field == "duration_ms" {
				n, err := strconv.ParseFloat(fmt.Sprint(v), 64)
				if err != nil || math.IsInf(n, 0) || math.IsNaN(n) {
					return nil, fmt.Errorf("invalid numeric trace filter")
				}
				return n, nil
			}
			n, err := strconv.ParseInt(fmt.Sprint(v), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid integer trace filter")
			}
			return n, nil
		}
		s, ok := v.(string)
		if !ok {
			return nil, tracePageUnsupported{"non-string dimension filter requires canonical projection"}
		}
		return s, nil
	}
	left, placeholder := expression, "?"
	if !numeric {
		left, placeholder = "BINARY "+expression, "BINARY ?"
	}
	if op == "IN" || op == "NOT IN" {
		values := nativeFilterValues(f.Value)
		if values == nil {
			return "", nil, fmt.Errorf("IN trace filter requires an array")
		}
		if len(values) > 200 {
			return "", nil, tracePageUnsupported{"IN trace filter supports at most 200 values"}
		}
		if len(values) == 0 {
			if op == "IN" {
				return "FALSE", nil, nil
			}
			return "TRUE", nil, nil
		}
		clauses, args := []string{}, []any{}
		for _, value := range values {
			converted, err := convert(value)
			if err != nil {
				return "", nil, err
			}
			clauses = append(clauses, left+" <=> "+placeholder)
			args = append(args, converted)
		}
		clause := "(" + strings.Join(clauses, " OR ") + ")"
		if op == "NOT IN" {
			clause = "NOT " + clause
		}
		return clause, args, nil
	}
	value, err := convert(f.Value)
	if err != nil {
		return "", nil, err
	}
	switch op {
	case "=", "!=":
		clause := "(" + left + " <=> " + placeholder + ")"
		if op == "!=" {
			clause = "NOT " + clause
		}
		return clause, []any{value}, nil
	case ">", ">=", "<", "<=":
		return "COALESCE(" + left + " " + op + " " + placeholder + ", FALSE)", []any{value}, nil
	case "LIKE":
		if numeric {
			return "", nil, tracePageUnsupported{"numeric LIKE requires canonical projection"}
		}
		if value == nil {
			return "FALSE", nil, nil
		}
		// Canonical LIKE recognizes % only; underscore and backslash are literal.
		pattern := strings.NewReplacer("!", "!!", "_", "!_").Replace(value.(string))
		return "COALESCE(" + left + " LIKE " + placeholder + " ESCAPE '!', FALSE)", []any{pattern}, nil
	default:
		return "", nil, tracePageUnsupported{"unsupported trace page operator: " + f.Op}
	}
}

func tracePageFilter(f nativeFilter) (string, []any, error) {
	if column, ok := tracePageColumns[f.Field]; ok {
		return tracePageCompare(column, f.Field, f)
	}
	dimension, ok := tracePageDimensions[f.Field]
	if !ok {
		return "", nil, tracePageUnsupported{"unsupported trace page filter: " + f.Field}
	}
	comparison, args, err := tracePageCompare("filter_value.value", f.Field, f)
	if err != nil {
		return "", nil, err
	}
	missing, missingArgs, err := tracePageCompare("NULL", f.Field, f)
	if err != nil {
		return "", nil, err
	}
	prefix := "SELECT 1 FROM dimension_values filter_value WHERE BINARY filter_value.trace_id = BINARY s.trace_id AND filter_value.dimension = '" + dimension + "'"
	op := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(f.Op), "_", " "))
	negative := op == "!=" || op == "NEQ" || op == "NOT IN" || op == "IS NULL"
	exists := "EXISTS (" + prefix + " AND " + comparison + ")"
	if negative {
		exists = "(EXISTS (" + prefix + ") AND NOT EXISTS (" + prefix + " AND NOT (" + comparison + ")))"
	}
	return "(" + exists + " OR (NOT EXISTS (" + prefix + ") AND " + missing + "))", append(args, missingArgs...), nil
}

func buildTracePageSQL(table, org string, p nativeListParams, maxSpanCount int) (string, []any, error) {
	if maxSpanCount <= 0 {
		maxSpanCount = math.MaxInt
	}
	if p.Page < 1 || p.Limit < 1 || p.Limit > 200 || int64(p.Page-1) > math.MaxInt64/int64(p.Limit) {
		return "", nil, fmt.Errorf("invalid trace page bounds")
	}
	if p.SessionID != "" || len(p.SessionIDs) != 0 || p.InputKeyword != "" {
		return "", nil, tracePageUnsupported{"session and IO queries require canonical projection"}
	}
	if p.SearchType != "" && p.SearchType != "traceId" && p.SearchType != "trace_id" {
		return "", nil, tracePageUnsupported{"unsupported trace page search: " + p.SearchType}
	}
	order, err := tracePageOrder(p.OrderBy)
	if err != nil {
		return "", nil, err
	}
	physical := nativeListParams{From: p.From, To: p.To, User: p.User, SpaceID: p.SpaceID, AgentProduct: p.AgentProduct}
	where, args, err := nativeTraceWhere(org, physical)
	if err != nil {
		return "", nil, err
	}
	filters := append([]nativeFilter{}, p.Filters...)
	for _, direct := range []struct{ field, value string }{{"trace_id", p.TraceID}, {"env", p.Env}, {"scene", p.Scene}, {"username", p.Username}, {"resource.attributes.aivision.agent.id", p.AgentUUID}} {
		if direct.value != "" {
			filters = append(filters, nativeFilter{Field: direct.field, Op: "=", Value: direct.value})
		}
	}
	if len(p.TraceIDs) > 0 {
		filters = append(filters, nativeFilter{Field: "trace_id", Op: "in", Value: p.TraceIDs})
	}
	if len(filters) > 100 {
		return "", nil, tracePageUnsupported{"filters supports at most 100 items"}
	}
	conditions := []string{"TRUE"}
	filterArgs := []any{}
	for _, f := range filters {
		clause, values, err := tracePageFilter(f)
		if err != nil {
			return "", nil, err
		}
		conditions = append(conditions, clause)
		filterArgs = append(filterArgs, values...)
	}
	if p.Keyword != "" {
		if len(p.Keyword) > 4096 {
			return "", nil, tracePageUnsupported{"keyword is too long"}
		}
		conditions = append(conditions, "LOCATE(BINARY LOWER(?), BINARY LOWER(s.trace_id)) > 0")
		filterArgs = append(filterArgs, p.Keyword)
	}
	if len(p.Tags) > 0 {
		if len(p.Tags) > 100 {
			return "", nil, tracePageUnsupported{"tags supports at most 100 values"}
		}
		clause, values, err := tracePageCompare("tag_filter.value", "tags", nativeFilter{Op: "in", Value: p.Tags})
		if err != nil {
			return "", nil, err
		}
		conditions = append(conditions, "EXISTS (SELECT 1 FROM dimension_values tag_filter WHERE BINARY tag_filter.trace_id=BINARY s.trace_id AND tag_filter.dimension='tags' AND "+clause+")")
		filterArgs = append(filterArgs, values...)
	}
	compact := func(key string) string { return pageJSONKey(ingest.TracePageDimensionsColumn, key) }
	validString := func(expr string) string {
		return "(" + expr + " IS NULL OR JSON_TYPE(" + expr + ") IN ('NULL','STRING'))"
	}
	query := `WITH base AS (
SELECT t.trace_id,t.span_id,t.start_time_unix_nano,t.end_time_unix_nano,t.status_code,` + ingest.TracePageSourceBytesColumn + ` AS source_bytes,
` + compact("space") + ` AS scope_space,` + pageText(compact("agent")) + ` AS scope_agent,
` + compact("env") + ` AS env_json,` + compact("scene") + ` AS scene_json,
` + compact("resource_username") + ` AS resource_username,` + compact("attribute_username") + ` AS attribute_username,
` + compact("has_resource_username") + ` AS has_resource_username,` + compact("has_attribute_username") + ` AS has_attribute_username,
` + compact("has_env") + ` AS has_env,` + compact("has_scene") + ` AS has_scene,` + compact("has_agent") + ` AS has_agent,
` + compact("tags") + ` AS tags_json,` + compact("has_tags") + ` AS has_tags
FROM ` + table + ` t WHERE ` + where + `
), summaries AS (
SELECT trace_id, MIN(start_time_unix_nano) AS start_ns, MAX(end_time_unix_nano) AS end_ns,
(CAST((CAST(MAX(end_time_unix_nano) AS DECIMAL(30,0))-CAST(MIN(start_time_unix_nano) AS DECIMAL(30,0)))/1000000000.0 AS DOUBLE)*1000) AS duration_ms,
COUNT(*) AS span_count, MAX(CASE WHEN status_code=2 THEN 2 WHEN status_code=1 THEN 1 ELSE 0 END) AS status_code,
SUM(has_resource_username) AS resource_username_count,
CASE WHEN COUNT(*) != COUNT(DISTINCT BINARY span_id) OR SUM(CASE WHEN span_id IS NULL OR BINARY span_id=BINARY '' OR start_time_unix_nano IS NULL OR BINARY trace_id=BINARY '' OR NOT (BINARY ` + pageText("scope_space") + ` <=> BINARY ?) THEN 1 ELSE 0 END)>0
OR (BINARY ? != BINARY '' AND (COUNT(DISTINCT BINARY scope_agent)!=1 OR SUM(CASE WHEN scope_agent IS NULL OR BINARY scope_agent=BINARY '' THEN 1 ELSE 0 END)>0)) THEN 1 ELSE 0 END AS invalid_scope,
SUM(CASE WHEN ` + validString("env_json") + ` AND ` + validString("scene_json") + ` AND ` + validString("resource_username") + ` AND ` + validString("attribute_username") + ` THEN 0 ELSE 1 END) AS unsupported_values
FROM base GROUP BY BINARY trace_id,trace_id
), tag_updates AS (
SELECT trace_id, tags_json, ROW_NUMBER() OVER (PARTITION BY BINARY trace_id ORDER BY COALESCE(end_time_unix_nano,start_time_unix_nano) DESC, start_time_unix_nano DESC, BINARY span_id DESC) AS update_rank
FROM base WHERE has_tags=1
), decoded_tags AS (
SELECT trace_id, CASE WHEN JSON_TYPE(tags_json)='STRING' AND JSON_VALID(JSON_UNQUOTE(tags_json)) THEN JSON_EXTRACT(JSON_UNQUOTE(tags_json),'$') ELSE tags_json END AS tags
FROM tag_updates WHERE update_rank=1
), canonical_tags AS (
SELECT trace_id, CASE WHEN tags IS NULL OR JSON_TYPE(tags)='NULL' THEN JSON_ARRAY() WHEN JSON_TYPE(tags)='ARRAY' THEN tags WHEN JSON_TYPE(tags)='STRING' THEN JSON_ARRAY(JSON_UNQUOTE(tags)) ELSE JSON_ARRAY() END AS tags,
CASE WHEN tags IS NULL OR JSON_TYPE(tags) IN ('NULL','ARRAY','STRING') THEN 0 ELSE 1 END AS unsupported_values FROM decoded_tags
), tag_values AS (
SELECT /*+ MATERIALIZE */ c.trace_id, j.tag_value AS value,
CASE WHEN JSON_TYPE(JSON_EXTRACT(c.tags,CONCAT('$[',j.tag_ord-1,']')))='STRING' AND CHAR_LENGTH(JSON_UNQUOTE(JSON_EXTRACT(c.tags,CONCAT('$[',j.tag_ord-1,']'))))<=16384 THEN 0 ELSE 1 END AS unsupported_values
FROM canonical_tags c, JSON_TABLE(c.tags, '$[*]' COLUMNS (tag_ord FOR ORDINALITY, tag_value VARCHAR(16384) PATH '$' NULL ON ERROR)) j
), dimension_values AS (
SELECT /*+ MATERIALIZE */ trace_id,'env' AS dimension,` + pageText("env_json") + ` AS value FROM base WHERE has_env=1
UNION ALL SELECT trace_id,'scene',` + pageText("scene_json") + ` FROM base WHERE has_scene=1
UNION ALL SELECT b.trace_id,'username',CASE WHEN s.resource_username_count>0 THEN ` + pageText("resource_username") + ` ELSE ` + pageText("attribute_username") + ` END FROM base b JOIN summaries s ON BINARY b.trace_id=BINARY s.trace_id WHERE (s.resource_username_count>0 AND b.has_resource_username=1) OR (s.resource_username_count=0 AND b.has_attribute_username=1)
UNION ALL SELECT trace_id,'agent_uuid',scope_agent FROM base WHERE has_agent=1
UNION ALL SELECT trace_id,'tags',value FROM tag_values
), matching AS (
SELECT /*+ MATERIALIZE */ s.* FROM summaries s WHERE ` + strings.Join(conditions, " AND ") + `
), page AS (
SELECT s.*, ROW_NUMBER() OVER (ORDER BY ` + order + `) AS page_rank FROM matching s ORDER BY ` + order + ` LIMIT ? OFFSET ?
), facet_counts AS (
SELECT v.dimension, v.value, COUNT(DISTINCT BINARY v.trace_id) AS trace_count FROM dimension_values v JOIN matching m ON BINARY v.trace_id=BINARY m.trace_id WHERE v.dimension IN ('env','scene','username','tags') AND v.value IS NOT NULL AND (v.dimension='tags' OR BINARY v.value!=BINARY '') GROUP BY v.dimension,BINARY v.value,v.value
)
SELECT CAST(JSON_ARRAYAGG(JSON_OBJECT('kind',kind,'result',result,'section',section,'pageRank',page_rank)) AS CHAR CHARACTER SET utf8mb4) AS trace_page_rows FROM (
SELECT 0 AS section, 0 AS page_rank, '' AS trace_key, 0 AS span_start, '' AS span_key, 'meta' AS kind,
JSON_OBJECT('totalItems',(SELECT COUNT(*) FROM matching),'invalidScope',COALESCE((SELECT SUM(invalid_scope) FROM summaries),0),'oversizedTraces',(SELECT COUNT(*) FROM page WHERE span_count > ?),'unsupportedValues',COALESCE((SELECT SUM(unsupported_values) FROM summaries),0)+COALESCE((SELECT SUM(unsupported_values) FROM canonical_tags),0)+COALESCE((SELECT SUM(unsupported_values) FROM tag_values),0)) AS result
UNION ALL SELECT 1,0,'',0,'','facet',JSON_OBJECT('dimension',dimension,'values',JSON_ARRAYAGG(JSON_OBJECT('value',value,'count',trace_count))) FROM facet_counts GROUP BY dimension
UNION ALL SELECT 2,p.page_rank,p.trace_id,0,'','trace',JSON_OBJECT('traceId',p.trace_id,'spanCount',p.span_count,'spans',JSON_ARRAYAGG(` + tracePageSpanJSON() + `)) FROM ` + table + ` b JOIN page p ON BINARY b.trace_id=BINARY p.trace_id WHERE ` + where + ` AND p.span_count<=? GROUP BY p.page_rank,p.trace_id,p.span_count
) page_result`
	args = append(args, p.SpaceID, p.AgentUUID)
	args = append(args, filterArgs...)
	args = append(args, p.Limit, int64(p.Page-1)*int64(p.Limit), maxSpanCount)
	_, rawArgs, _ := nativeTraceWhere(org, physical)
	args = append(args, rawArgs...)
	args = append(args, maxSpanCount)
	return query, args, nil
}

func tracePageSpanJSON() string {
	parts := []string{}
	for _, column := range []string{"trace_id", "span_id", "parent_span_id", "span_name", "span_kind", "status_code", "status_message", "start_time_unix_nano", "end_time_unix_nano", "duration_nano", "service_name", "user_id", "space_id", "agent_product", "session_id", "resource_attributes", "attributes", "events", "links", "payload"} {
		key, expr := column, "b."+column
		if column == "span_name" {
			key = "name"
		}
		if column == "span_kind" {
			key = "kind"
		}
		parts = append(parts, "'"+key+"'", expr)
	}
	return "JSON_OBJECT(" + strings.Join(parts, ",") + ")"
}

// Shared across native read endpoints, including raw recovery paths. Admission
// stays held through serialization and response writing; replicas multiply it.
var traceQuerySlots = make(chan struct{}, 2)

func acquireTraceQuery(w http.ResponseWriter) bool {
	select {
	case traceQuerySlots <- struct{}{}:
		return true
	default:
		writeTraceCapacityError(w, http.StatusTooManyRequests, "Trace query capacity is busy; retry shortly")
		return false
	}
}
func releaseTraceQuery() { <-traceQuerySlots }

var errTracePageBudget = errors.New("trace page exceeds resource budget")

func writeTraceCapacityError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	if status == http.StatusTooManyRequests {
		w.Header().Set("Retry-After", "1")
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "error", "error": message})
}
