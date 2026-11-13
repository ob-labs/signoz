package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const MediaOriginalsAttribute = "aivision.media.originals"

var mediaDirections = map[string]string{
	"langfuse.observation.input": "input", "langfuse.trace.input": "input", "gen_ai.input.messages": "input", "gen_ai.tool.call.arguments": "input", "input.value": "input", "aivision.request.input": "input",
	"langfuse.observation.output": "output", "langfuse.trace.output": "output", "gen_ai.output.messages": "output", "gen_ai.tool.call.result": "output", "output.value": "output", "aivision.request.output": "output",
}

func decodeMediaObject(raw any) (map[string]any, error) {
	text, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("invalid trace object")
	}
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var obj map[string]any
	if err := dec.Decode(&obj); err != nil {
		return nil, err
	}
	if obj == nil {
		obj = map[string]any{}
	}
	return obj, nil
}

// prepareTraceMediaRows runs within the trace transaction. Neither the caller's
// original rows nor the source OTLP are mutated before a successful commit.
func prepareTraceMediaRows(ctx context.Context, tx *sql.Tx, prefix string, rows [][]any) ([][]any, error) {
	result := make([][]any, len(rows))
	for i, row := range rows {
		result[i] = append([]any(nil), row...)
		if len(row) != 31 {
			return nil, fmt.Errorf("invalid trace row contract")
		}
		attrs, err := decodeMediaObject(row[22])
		if err != nil {
			return nil, err
		}
		resource, err := decodeMediaObject(row[21])
		if err != nil {
			return nil, err
		}
		// Never trust an incoming restoration capability.
		delete(attrs, MediaOriginalsAttribute)
		agent, _ := resource["aivision.agent.id"].(string)
		originals := map[string]string{}
		keys := make([]string, 0, len(attrs))
		for key := range attrs {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			direction, known := mediaDirections[key]
			raw, ok := attrs[key].(string)
			if !known || !ok || agent == "" {
				continue
			}
			scope := mediaScope{fmt.Sprint(row[0]), fmt.Sprint(row[1]), agent, fmt.Sprint(row[3]), fmt.Sprint(row[4]), direction}
			projected, err := persistProjectedTraceAttribute(ctx, tx, prefix, scope, key, raw)
			if err != nil {
				return nil, err
			}
			if projected != raw {
				attrs[key] = projected
				originals[key] = traceMediaOriginalID(scope, key, projected, raw)
			}
		}
		if len(originals) > 0 {
			attrs[MediaOriginalsAttribute] = originals
		}
		payload, err := decodeMediaObject(row[25])
		if err != nil {
			return nil, err
		}
		payload["attributes"] = attrs
		a, err := json.Marshal(attrs)
		if err != nil {
			return nil, err
		}
		p, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		result[i][22], result[i][25] = string(a), string(p)
	}
	return result, nil
}
