package aivision

import (
	"encoding/json"
	"strings"
)

// Metadata callers request a closed set of canonical fields. Removing only
// private maps leaves arbitrary flattened SDK attributes in every list row.
// Project in SQL so unused raw bodies never enter Go or the response.
func readModelRowJSONSQL(metadataOnly bool) string {
	if !metadataOnly {
		return "JSON_REMOVE(p.row_json,'$._operation_previews','$._statistics','$._metrics')"
	}
	fields := []string{
		"trace_id", "name", "userId", "session_id", "environment", "environmentSource",
		"start_time_unix_nano", "end_time_unix_nano", "duration_ms", "status_code", "span_count",
		"usage", "coverage", "costCoverage", "reportedUsage", "rootObservationId", "rootSelectionReason", "missingParentIds",
		"input_tokens", "output_tokens", "total_tokens",
		"attributes.gen_ai.span.kind", "attributes.gen_ai.request.model",
		"attributes.gen_ai.usage.input_tokens", "attributes.gen_ai.usage.output_tokens", "attributes.gen_ai.usage.total_tokens",
		"attributes.langfuse.trace.tags", "resource.attributes.ant.agent.env", "resource.attributes.ant.agent.scene",
		"resource.attributes.aivision.agent.id", "resource.attributes.aivision.space.id", "resource.attributes.ant.agent.product",
		"attributes.ant.username", "attributes.gen_ai.tool.call.name", "attributes.gen_ai.operation.name", "attributes.gen_ai.provider.name",
		"_traceTags", "_sceneValues", "_contentComplete",
	}
	values := make([]string, 0, (len(fields)+4)*2)
	for _, field := range fields {
		// Quoted JSON path members address literal dotted keys, rather than
		// interpreting native aliases as nested objects. All keys are constants.
		path, _ := json.Marshal(field)
		values = append(values, "'"+field+"'", "JSON_EXTRACT(p.row_json,'$."+string(path)+"')")
	}
	// Legacy validators require these canonical keys. Explicit null preserves
	// their shape without selecting a full body; ioPreview is a separate column.
	for _, field := range []string{"attributes.gen_ai.input.messages", "attributes.gen_ai.output.messages", "attributes.gen_ai.tool.call.arguments", "attributes.gen_ai.tool.call.result"} {
		values = append(values, "'"+field+"'", "NULL")
	}
	return "JSON_OBJECT(" + strings.Join(values, ",") + ")"
}
