package aivision

const traceMetricsWindowContract = "native-trace-metrics-window-v1"

// This private representation contains canonical aggregation facts and the
// dimensions on which metrics may filter. It intentionally cannot serve content
// searches or IO previews; callers use the complete raw window for those queries.
var traceMetricsWindowFields = []string{
	"trace_id", "name", "userId", "session_id", "environment", "environmentSource",
	"start_time_unix_nano", "end_time_unix_nano", "duration_ms", "status_code", "span_count",
	"usage", "coverage", "reportedUsage", "rootObservationId", "rootSelectionReason", "missingParentIds",
	"input_tokens", "output_tokens", "total_tokens",
	"attributes.gen_ai.usage.input_tokens", "attributes.gen_ai.usage.output_tokens", "attributes.gen_ai.usage.total_tokens",
	"attributes.langfuse.trace.tags", "resource.attributes.aivision.space.id", "resource.attributes.aivision.agent.id",
	"_traceTags", "_sceneValues", "_metrics",
}

func traceMetricsWindowRow(row map[string]any) map[string]any {
	compact := make(map[string]any, len(traceMetricsWindowFields)+1)
	for _, field := range traceMetricsWindowFields {
		// Preserve absence separately from an explicitly reported JSON null. A
		// missing required fact must remain detectable by the canonical consumer.
		if value, exists := row[field]; exists {
			compact[field] = value
		}
	}
	return compact
}
