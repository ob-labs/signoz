package aivision

import (
	"fmt"
	"strings"
)

// frameworkDashboardPrefix namespaces the traces-backed dashboard queries used
// by the agent-framework / langfuse-sdk / nvidia-nemo-relay dashboards. Those
// products emit OTel spans only (no OTLP logs), so their event stream is
// derived from the traces table instead of the logs table the CLI-agent families
// (codex / claude_code / common / ...) read from.
const frameworkDashboardPrefix = "framework."

// frameworkSpanEventKindSQL classifies a single span into a semantic event kind
// directly in SQL, so the browser never has to fetch every span's attributes and
// run the classification client-side (which was an N+1 that cannot scale to large
// spaces). Signal precedence, verified against real span attributes:
//
//  1. gen_ai.operation.name (OTel standard): execute_tool -> tool; chat /
//     text_completion -> llm.
//  2. gen_ai.tool.call.name present -> tool.
//  3. langfuse.observation.type (present on every langfuse span): tool -> tool;
//     generation -> llm.
//  4. gen_ai.request.model present or any usage tokens > 0 -> llm.
//  5. anything else -> "other".
//
// Chain / agent wrapper spans (e.g. `model` wrapping `ChatOpenAI`, `tools`
// wrapping `get_weather`) carry none of the model/token/operation signals above,
// so they fall through to "other" and are filtered out by the event stream. That
// is what collapses a run's nested spans to one row per real LLM / tool call,
// matching the per-request density of the CLI-agent event streams.
func frameworkSpanEventKindSQL() string {
	op := "LOWER(" + traceAttributeTextSQL("gen_ai.operation.name") + ")"
	lfType := "LOWER(" + traceAttributeTextSQL("langfuse.observation.type") + ")"
	toolName := traceToolNameSQL()
	model := traceModelSQL()
	inputTokens := inputTokenValueSQL()
	outputTokens := outputTokenValueSQL()
	return "CASE " +
		"WHEN " + op + " = 'execute_tool' OR NULLIF(" + toolName + ", '') IS NOT NULL OR " + lfType + " = 'tool' THEN 'tool' " +
		"WHEN " + op + " IN ('chat', 'text_completion') OR " + lfType + " = 'generation' OR NULLIF(" + model + ", '') IS NOT NULL OR " + inputTokens + " > 0 OR " + outputTokens + " > 0 THEN 'llm' " +
		"ELSE 'other' END"
}

// frameworkDashboardWhere extends the shared dashboard scope filter (org / space
// / user / agent_product / time range) with the single-agent-instance drilldown
// the framework dashboard needs. The instance id lives in resource_attributes
// under aivision.agent.id; it is passed through request.Params (the end-to-end
// passthrough channel) rather than widening dashboardWhere, so other families are
// unaffected.
func frameworkDashboardWhere(request dashboardRequest, where string, args []any) (string, []any) {
	agentID := strings.TrimSpace(fmt.Sprint(request.Params["agent_uuid"]))
	if agentID == "" || agentID == "<nil>" {
		return where, args
	}
	return where + " AND " + traceResourceTextSQL("aivision.agent.id") + " = ?", append(args, agentID)
}

// buildFrameworkDashboardStatement implements the framework (traces-backed)
// dashboard contract. Like the other builders it accepts allow-listed query keys
// only, never caller-provided SQL.
func buildFrameworkDashboardStatement(store *oceanBaseStore, request dashboardRequest, where string, args []any) (dashboardStatement, bool, error) {
	if !strings.HasPrefix(request.QueryKey, frameworkDashboardPrefix) {
		return dashboardStatement{}, false, nil
	}
	suffix := strings.TrimPrefix(request.QueryKey, frameworkDashboardPrefix)

	table := store.tracesTable()
	scopeWhere, scopeArgs := frameworkDashboardWhere(request, where, args)
	kindSQL := frameworkSpanEventKindSQL()
	eventCondition := "(" + kindSQL + ") IN ('llm', 'tool')"

	statement := dashboardStatement{}
	switch suffix {
	case "overview.event_stream":
		limit := dashboardLimit(request.Params, store.cfg.MaxLimit)
		statement.SQL = `SELECT
(start_time_unix_nano DIV 1000000) AS time,
span_id,
trace_id,
span_name AS name,
` + kindSQL + ` AS kind,
duration_nano / 1000000 AS cost_ms,
` + traceModelSQL() + ` AS model,
` + inputTokenValueSQL() + ` AS input_tokens,
` + outputTokenValueSQL() + ` AS output_tokens
FROM ` + table + ` WHERE ` + scopeWhere + ` AND ` + eventCondition + `
ORDER BY start_time_unix_nano DESC LIMIT ?`
		statement.Args = append(scopeArgs, limit)
		statement.Columns = mixedDashboardColumns(
			dashboardColumn{Key: "time", Type: "timestamp"},
			dashboardColumn{Key: "span_id", Type: "string"},
			dashboardColumn{Key: "trace_id", Type: "string"},
			dashboardColumn{Key: "name", Type: "string"},
			dashboardColumn{Key: "kind", Type: "string"},
			dashboardColumn{Key: "cost_ms", Type: "number"},
			dashboardColumn{Key: "model", Type: "string"},
			dashboardColumn{Key: "input_tokens", Type: "number"},
			dashboardColumn{Key: "output_tokens", Type: "number"},
		)

	case "events.timeseries":
		step := dashboardStepMS(request.Params, request.To-request.From)
		statement.SQL = `SELECT
(((start_time_unix_nano DIV 1000000) DIV ?) * ?) AS time,
` + kindSQL + ` AS kind,
COUNT(*) AS count
FROM ` + table + ` WHERE ` + scopeWhere + ` AND ` + eventCondition + `
GROUP BY time, kind ORDER BY time ASC`
		statement.Args = append([]any{step, step}, scopeArgs...)
		statement.Columns = mixedDashboardColumns(
			dashboardColumn{Key: "time", Type: "timestamp"},
			dashboardColumn{Key: "kind", Type: "string"},
			dashboardColumn{Key: "count", Type: "number"},
		)

	default:
		return dashboardStatement{}, false, nil
	}
	return statement, true, nil
}
