package aivision

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
)

const sessionReadModelContract = "native-session-read-model-v1"
const sessionPageTraceLimit = 5000
const sessionPagePayloadLimit = 16 << 20

// Session lists consume canonical summary evidence only. A whitelist also
// excludes duplicated SDK input/output attributes and future raw aliases.
func sessionSummarySQL() string {
	fields := []string{"trace_id", "name", "userId", "session_id", "environment", "environmentSource", "start_time_unix_nano", "end_time_unix_nano", "duration_ms", "status_code", "span_count", "usage", "coverage", "reportedUsage", "rootObservationId", "rootSelectionReason", "missingParentIds", "total_tokens", "_traceTags", "_sceneValues", "resource.attributes.aivision.agent.id", "resource.attributes.aivision.space.id", "resource.attributes.ant.agent.product", "attributes.gen_ai.request.model"}
	values := make([]string, 0, len(fields)*2)
	for _, field := range fields {
		path, _ := json.Marshal(field)
		values = append(values, "'"+field+"'", "JSON_EXTRACT(p.row_json,'$."+string(path)+"')")
	}
	return "JSON_OBJECT(" + strings.Join(values, ",") + ")"
}

type sessionReadModelParams struct {
	nativeListParams
	SessionKeyword string
	SessionStatus  string
	MinTraceCount  *int64
	MinTotalTokens *int64
	MinDurationMS  *float64
}

// The canonical session endpoint intentionally has no raw-Span filter DSL.
func (h *handler) sessionReadModelParams(r *http.Request) (sessionReadModelParams, error) {
	allowed := map[string]bool{}
	for _, k := range []string{"from", "to", "space_id", "agent_uuid", "agent_product", "user", "page", "limit", "projection_context", "session_semantics", "session_id", "session_keyword", "session_status", "min_trace_count", "min_total_tokens", "min_duration_ms", "env", "username", "tags", "keyword", "searchType", "search_type", "orderBy", "order_by"} {
		allowed[k] = true
	}
	for k, values := range r.URL.Query() {
		if !allowed[k] || len(values) != 1 {
			return sessionReadModelParams{}, fmt.Errorf("unsupported or repeated session parameter: %s", k)
		}
	}
	base, err := h.nativeParams(r)
	if err != nil {
		return sessionReadModelParams{}, err
	}
	q := r.URL.Query()
	p := sessionReadModelParams{nativeListParams: base, SessionKeyword: q.Get("session_keyword"), SessionStatus: q.Get("session_status")}
	p.AgentUUID, p.ProjectionContext = q.Get("agent_uuid"), q.Get("projection_context")
	// SDK dimensions and content are literal values, not whitespace-normalized
	// physical identities. Preserve the exact canonical Python predicates.
	p.SessionID, p.Env, p.Username, p.Keyword = q.Get("session_id"), q.Get("env"), q.Get("username"), q.Get("keyword")
	if raw := q.Get("tags"); strings.HasPrefix(strings.TrimSpace(raw), "[") {
		// The legacy ID-list parser trims values; canonical JSON facet values
		// must match the recorded tag bytes, as on the Trace read-model route.
		if err := json.Unmarshal([]byte(raw), &p.Tags); err != nil {
			return p, fmt.Errorf("invalid session tags")
		}
	}
	if _, camel := q["orderBy"]; camel {
		if _, snake := q["order_by"]; snake {
			return p, fmt.Errorf("use one session ordering parameter")
		}
	}
	if p.OrderBy != "" && p.OrderBy != "last_activity.desc" && p.OrderBy != "end_time_unix_nano.desc" {
		return p, fmt.Errorf("canonical sessions require last activity descending")
	}
	// Python freezes its historical end-time alias. Both supported names use
	// the one canonical session activity ordering, including unfinished turns.
	p.OrderBy = "last_activity.desc"
	if p.Limit > 50 || p.Page > 1000000 || p.From < 0 || p.To <= p.From || p.To > math.MaxInt64/1000000 {
		return p, fmt.Errorf("invalid canonical session page or window")
	}
	if v := q.Get("session_semantics"); v != "" && v != "canonical-trace-v1" {
		return p, fmt.Errorf("unsupported session semantics")
	}
	if !projectionContextPattern.MatchString(p.ProjectionContext) || p.AgentUUID == "" {
		return p, fmt.Errorf("canonical session projection context and Agent UUID required")
	}
	for _, v := range []string{p.AgentUUID, p.User, p.AgentProduct, p.SessionID, p.Env, p.Username, p.SessionKeyword} {
		if len(v) > 255 {
			return p, fmt.Errorf("session dimension exceeds limit")
		}
	}
	if p.SessionStatus != "" && p.SessionStatus != "error" && p.SessionStatus != "ok" && p.SessionStatus != "unset" {
		return p, fmt.Errorf("invalid session_status")
	}
	if len(p.Keyword) > 4096 || p.Keyword != "" && p.SearchType != "input" && p.SearchType != "output" {
		return p, fmt.Errorf("canonical session keyword requires input/output searchType")
	}
	for _, v := range p.Tags {
		if v == "" || len(v) > 1024 {
			return p, fmt.Errorf("invalid session tag")
		}
	}
	if len(p.Tags) > 100 {
		return p, fmt.Errorf("too many session tags")
	}
	for key, dst := range map[string]**int64{"min_trace_count": &p.MinTraceCount, "min_total_tokens": &p.MinTotalTokens} {
		if raw, exists := q[key]; exists {
			v, e := strconv.ParseInt(raw[0], 10, 64)
			if e != nil || v < 0 || key == "min_trace_count" && v == 0 {
				return p, fmt.Errorf("invalid %s", key)
			}
			*dst = &v
		}
	}
	if raw, exists := q["min_duration_ms"]; exists {
		v, e := strconv.ParseFloat(raw[0], 64)
		if e != nil || v < 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			return p, fmt.Errorf("invalid min_duration_ms")
		}
		p.MinDurationMS = &v
	}
	return p, nil
}

func sessionScopeSQL(org string, p sessionReadModelParams) (string, []any, error) {
	base := p.nativeListParams
	base.SessionID, base.SessionIDs, base.TraceID, base.TraceIDs, base.Fields = "", nil, "", nil, nil
	base.Keyword, base.InputKeyword, base.Env, base.Scene, base.Username = "", "", "", "", ""
	base.Tags, base.Filters = nil, nil
	where, args, err := readModelWhere(org, base)
	if err != nil {
		return "", nil, err
	}
	where += " AND p.session_id<>''"
	if p.SessionID != "" {
		where += " AND BINARY p.session_id=BINARY ?"
		args = append(args, p.SessionID)
	}
	if p.SessionKeyword != "" {
		where += " AND LOCATE(BINARY ?,BINARY p.session_id)>0"
		args = append(args, p.SessionKeyword)
	}
	return where, args, nil
}

func sessionMatchSQL(p sessionReadModelParams, excluded string) (string, []any) {
	clauses := []string{"TRUE"}
	args := []any{}
	for _, dim := range []struct{ key, column, value string }{{"env", "environment", p.Env}, {"username", "username", p.Username}} {
		if excluded != dim.key && dim.value != "" {
			clauses = append(clauses, "BINARY p."+dim.column+"=BINARY ?")
			args = append(args, dim.value)
		}
	}
	if excluded != "tags" && len(p.Tags) > 0 {
		tags := []string{}
		for _, tag := range p.Tags {
			value, _ := json.Marshal(tag)
			tags = append(tags, "JSON_CONTAINS(p.tags_json,?)")
			args = append(args, string(value))
		}
		clauses = append(clauses, "("+strings.Join(tags, " OR ")+")")
	}
	if p.Keyword != "" {
		column := "input_text"
		if p.SearchType == "output" {
			column = "output_text"
		}
		clauses = append(clauses, "LOCATE(BINARY ?,BINARY p."+column+")>0")
		args = append(args, p.Keyword)
	}
	return strings.Join(clauses, " AND "), args
}

// The match bit chooses sessions; WHERE never removes their unrelated turns.
func sessionGroupSQL(org, table string, p sessionReadModelParams, excluded string) (string, []any, error) {
	return sessionGroupSQLWithStrategy(org, table, p, excluded, false)
}

func (s *oceanBaseStore) sessionGroupSQL(org, table string, p sessionReadModelParams, excluded string) (string, []any, error) {
	return sessionGroupSQLWithStrategy(org, table, p, excluded, s.sessionTokenQueryOptimized)
}

func (s *oceanBaseStore) sessionTokenQueryStrategy(p sessionReadModelParams) string {
	if s.sessionTokenQueryOptimized && p.MinTotalTokens != nil {
		return "json_table"
	}
	return "original"
}

func sessionGroupSQLWithStrategy(org, table string, p sessionReadModelParams, excluded string, optimized bool) (string, []any, error) {
	where, scopeArgs, err := sessionScopeSQL(org, p)
	if err != nil {
		return "", nil, err
	}
	match, matchArgs := sessionMatchSQL(p, excluded)
	billableCalls := "JSON_EXTRACT(p.row_json,'$.coverage.billableCalls')"
	totalTokens := "JSON_EXTRACT(p.row_json,'$.usage.total')"
	tokenUsageJoin := ""
	if optimized && p.MinTotalTokens != nil {
		// Keep JSON types and the original CASE/cast semantics: strings remain
		// strings and large integers must never pass through floating point.
		billableCalls, totalTokens = "token_usage.billable_calls", "token_usage.total_tokens"
		tokenUsageJoin = ",JSON_TABLE(p.row_json,'$' COLUMNS (billable_calls JSON PATH '$.coverage.billableCalls' NULL ON EMPTY ERROR ON ERROR,total_tokens JSON PATH '$.usage.total' NULL ON EMPTY ERROR ON ERROR)) token_usage"
	}
	inner := "SELECT p.session_id,p.start_ns,p.end_ns,p.status,CASE WHEN " + match + " THEN 1 ELSE 0 END AS matched,CASE WHEN " + billableCalls + "=0 THEN 0 WHEN " + billableCalls + ">0 AND JSON_TYPE(" + totalTokens + ") IN ('INTEGER','UNSIGNED INTEGER','DOUBLE','DECIMAL') THEN CAST(JSON_UNQUOTE(" + totalTokens + ") AS DECIMAL(65,0)) ELSE NULL END AS trace_tokens,CASE WHEN " + billableCalls + ">0 THEN 1 ELSE 0 END AS billable FROM " + table + " p" + tokenUsageJoin + " WHERE " + where
	query := "SELECT session_id,COUNT(*) AS trace_count,MAX(COALESCE(end_ns,start_ns)) AS last_activity FROM (" + inner + ") canonical GROUP BY BINARY session_id,session_id HAVING SUM(matched)>0"
	args := append(append([]any{}, matchArgs...), scopeArgs...)
	if p.SessionStatus != "" {
		switch p.SessionStatus {
		case "error":
			query += " AND SUM(status=2)>0"
		case "ok":
			query += " AND SUM(status=1)=COUNT(*)"
		case "unset":
			query += " AND SUM(status=2)=0 AND SUM(status=1)<COUNT(*)"
		}
	}
	if p.MinTraceCount != nil {
		query += " AND COUNT(*)>=?"
		args = append(args, *p.MinTraceCount)
	}
	if p.MinTotalTokens != nil {
		query += " AND COUNT(trace_tokens)=COUNT(*) AND SUM(billable)>0 AND SUM(trace_tokens)>=?"
		args = append(args, *p.MinTotalTokens)
	}
	if p.MinDurationMS != nil {
		query += " AND COUNT(end_ns)=COUNT(*) AND (MAX(end_ns)-MIN(start_ns))/1000000>=?"
		args = append(args, *p.MinDurationMS)
	}
	return query, args, nil
}

// Reuse exactly the raw/current-or-prior-scope coverage guard used by Trace
// pages. It runs in the same read transaction as group counts, pages and facets.
func (s *oceanBaseStore) sessionReadModelCoverage(ctx context.Context, tx *sql.Tx, org string, p sessionReadModelParams) (string, error) {
	var covered bool
	err := tx.QueryRowContext(ctx, "SELECT complete FROM "+s.readModelTable("trace_projection_coverage")+" WHERE org_id=? AND space_id=?", org, p.SpaceID).Scan(&covered)
	if err == sql.ErrNoRows || err == nil && !covered {
		return "historical_coverage_incomplete", nil
	}
	if err != nil {
		return "", err
	}
	from, to := p.From*1000000, p.To*1000000
	query := `SELECT COALESCE(SUM(CASE WHEN s.scope_valid=FALSE OR s.dirty=TRUE OR p.trace_id IS NULL OR p.source_revision<>s.source_revision OR p.semantics_version<>1 OR p.projection_context<>? THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN s.min_timestamp<? OR s.max_timestamp>=? THEN 1 ELSE 0 END),0) FROM ` + s.readModelTable("trace_sources") + ` s LEFT JOIN ` + s.readModelTable("trace_projections") + ` p ON p.org_id=s.org_id AND p.space_id=s.space_id AND p.trace_id=s.trace_id WHERE s.org_id=? AND s.space_id=? AND ((s.min_timestamp<? AND s.max_timestamp>=?) OR (p.period>=? AND p.period<?))`
	args := []any{p.ProjectionContext, from, to, org, p.SpaceID, to, from, p.From, p.To}
	if p.User != "" || p.AgentProduct != "" {
		current, prior := []string{}, []string{"p.trace_id IS NOT NULL"}
		currentArgs, priorArgs := []any{}, []any{}
		for _, dim := range []struct{ column, value string }{{"user_id", p.User}, {"agent_product", p.AgentProduct}} {
			if dim.value != "" {
				current = append(current, "BINARY s."+dim.column+"=BINARY ?")
				prior = append(prior, "BINARY p."+dim.column+"=BINARY ?")
				currentArgs = append(currentArgs, dim.value)
				priorArgs = append(priorArgs, dim.value)
			}
		}
		query += " AND (s.scope_valid=FALSE OR (" + strings.Join(current, " AND ") + ") OR (" + strings.Join(prior, " AND ") + "))"
		args = append(args, currentArgs...)
		args = append(args, priorArgs...)
	}
	var stale, boundary int64
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&stale, &boundary); err != nil {
		return "", err
	}
	if boundary > 0 {
		return "boundary_crossing_trace", nil
	}
	if stale > 0 {
		return "projection_coverage_stale", nil
	}
	return "", nil
}

func (s *oceanBaseStore) sessionReadModelPage(ctx context.Context, org string, p sessionReadModelParams) (any, error) {
	if !s.authoritativeIngest {
		return readModelUnsupported("authoritative_ingest_not_asserted"), nil
	}
	table := s.readModelTable("trace_projections")
	groupSQL, args, err := s.sessionGroupSQL(org, table, p, "")
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
	if reason, err := s.sessionReadModelCoverage(ctx, tx, org, p); err != nil {
		return nil, err
	} else if reason != "" {
		return readModelUnsupported(reason), nil
	}
	var total, windowTotal int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM ("+groupSQL+") sessions", args...).Scan(&total); err != nil {
		return nil, err
	}
	window := p
	window.SessionID, window.SessionKeyword, window.SessionStatus, window.Env, window.Username, window.Keyword = "", "", "", "", "", ""
	window.Tags = nil
	window.MinTraceCount, window.MinTotalTokens, window.MinDurationMS = nil, nil, nil
	windowSQL, windowArgs, err := sessionGroupSQL(org, table, window, "")
	if err != nil {
		return nil, err
	}
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM ("+windowSQL+") sessions", windowArgs...).Scan(&windowTotal); err != nil {
		return nil, err
	}
	pageArgs := append(append([]any{}, args...), p.Limit, (p.Page-1)*p.Limit)
	groups, err := scanReadModelRows(ctx, tx, groupSQL+" ORDER BY last_activity DESC,BINARY session_id ASC LIMIT ? OFFSET ?", pageArgs...)
	if err != nil {
		return nil, err
	}
	data := make([]map[string]any, 0, len(groups))
	pageTraces, payloadBytes := int64(0), 0
	scope, scopeArgs, err := sessionScopeSQL(org, p)
	if err != nil {
		return nil, err
	}
	match, matchArgs := sessionMatchSQL(p, "")
	for _, group := range groups {
		sid, ok := group["session_id"].(string)
		if !ok || sid == "" {
			return nil, fmt.Errorf("invalid session group identity")
		}
		count, err := asInt64(group["trace_count"])
		if err != nil || count < 1 {
			return nil, fmt.Errorf("invalid session group count")
		}
		pageTraces += count
		if pageTraces > sessionPageTraceLimit {
			return readModelUnsupported("session_page_trace_limit"), nil
		}
		// Check the selected group's bounded summaries before fetching them into
		// Go. The source documents can contain very large complete root IO; JSON
		// removal and byte accounting happen inside storage, not after decoding.
		prunedRow := sessionSummarySQL()
		sizeArgs := append(append([]any{}, scopeArgs...), sid)
		var selectedBytes int64
		if err := tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(OCTET_LENGTH("+prunedRow+")+OCTET_LENGTH(p.preview_json)+100),0) FROM "+table+" p WHERE "+scope+" AND BINARY p.session_id=BINARY ?", sizeArgs...).Scan(&selectedBytes); err != nil {
			return nil, err
		}
		if selectedBytes < 0 || selectedBytes > int64(sessionPagePayloadLimit-payloadBytes) {
			return readModelUnsupported("session_page_payload_limit"), nil
		}
		traceArgs := append(append([]any{}, matchArgs...), scopeArgs...)
		traceArgs = append(traceArgs, sid, sessionPageTraceLimit+1)
		rows, err := scanReadModelRows(ctx, tx, "SELECT "+prunedRow+" AS row_json,p.preview_json,p.source_revision,CASE WHEN "+match+" THEN 1 ELSE 0 END AS matched FROM "+table+" p WHERE "+scope+" AND BINARY p.session_id=BINARY ? ORDER BY p.start_ns ASC,BINARY p.trace_id ASC LIMIT ?", traceArgs...)
		if err != nil {
			return nil, err
		}
		if int64(len(rows)) != count {
			return nil, fmt.Errorf("incomplete canonical session group")
		}
		traces := make([]map[string]any, 0, len(rows))
		matched := []string{}
		for _, raw := range rows {
			canonical, e := tracePageJSON(raw["row_json"])
			if e != nil {
				return nil, fmt.Errorf("invalid session projection row: %w", e)
			}
			preview, e := tracePageJSON(raw["preview_json"])
			if e != nil {
				return nil, fmt.Errorf("invalid session preview row: %w", e)
			}
			canonical["ioPreview"], canonical["sourceRevision"] = preview, raw["source_revision"]
			canonical["attributes.gen_ai.input.messages"], canonical["attributes.gen_ai.output.messages"] = preview["input"], preview["output"]
			if canonical["session_id"] != sid {
				return nil, fmt.Errorf("session projection identity mismatch")
			}
			m, e := asInt64(raw["matched"])
			if e != nil {
				return nil, e
			}
			if m == 1 {
				id, ok := canonical["trace_id"].(string)
				if !ok {
					return nil, fmt.Errorf("invalid session Trace identity")
				}
				matched = append(matched, id)
			}
			encoded, e := json.Marshal(canonical)
			if e != nil {
				return nil, e
			}
			payloadBytes += len(encoded)
			if payloadBytes > sessionPagePayloadLimit {
				return readModelUnsupported("session_page_payload_limit"), nil
			}
			traces = append(traces, canonical)
		}
		data = append(data, map[string]any{"session_id": sid, "traceCount": count, "traces": traces, "matchedTraceIds": matched})
	}
	facets := nativeFacets{"env": {}, "username": {}, "tags": {}}
	for _, dim := range []struct{ key, column string }{{"env", "environment"}, {"username", "username"}, {"tags", "tags_json"}} {
		selected, selectedArgs, err := s.sessionGroupSQL(org, table, p, dim.key)
		if err != nil {
			return nil, err
		}
		candidate, candidateArgs := sessionMatchSQL(p, dim.key)
		value := "p." + dim.column
		join := ""
		if dim.key == "tags" {
			value = "j.value"
			join = ",JSON_TABLE(p.tags_json,'$[*]' COLUMNS (value VARCHAR(1024) PATH '$')) j"
		}
		query := "SELECT " + value + " AS value,COUNT(DISTINCT BINARY p.session_id) AS count FROM " + table + " p JOIN (" + selected + ") selected ON BINARY selected.session_id=BINARY p.session_id" + join + " WHERE " + scope + " AND " + candidate + " AND " + value + "<>'' GROUP BY BINARY " + value + "," + value + " ORDER BY BINARY value LIMIT 501"
		facetArgs := append(append(append([]any{}, selectedArgs...), scopeArgs...), candidateArgs...)
		rows, err := scanReadModelRows(ctx, tx, query, facetArgs...)
		if err != nil {
			return nil, err
		}
		if len(rows) > 500 {
			return readModelUnsupported("facet_cardinality_exceeded"), nil
		}
		for _, row := range rows {
			count, e := asInt64(row["count"])
			if e != nil || count < 1 {
				return nil, fmt.Errorf("invalid session facet count")
			}
			facets[dim.key] = append(facets[dim.key], nativeFacetValue{Value: row["value"], Count: int(count)})
		}
	}
	meta := map[string]any{"contract": sessionReadModelContract, "sessionSemantics": "canonical-trace-v1", "environmentSemantics": "root-span-resource-v1", "userIdentitySemantics": "trace-sdk-user-id-v1", "complete": true, "queryMode": "read_model", "projection_context": p.ProjectionContext, "from": p.From, "to": p.To, "space_id": p.SpaceID, "agent_product": p.AgentProduct, "user": p.User, "agent_uuid": p.AgentUUID, "page": p.Page, "limit": p.Limit, "totalItems": total, "totalPages": (total + p.Limit - 1) / p.Limit, "windowTotalItems": windowTotal}
	meta["sessionTokenQueryStrategy"] = s.sessionTokenQueryStrategy(p)
	result := map[string]any{"supported": true, "data": data, "facets": facets, "meta": meta}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	if len(encoded) > sessionPagePayloadLimit {
		return readModelUnsupported("session_page_payload_limit"), nil
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}
