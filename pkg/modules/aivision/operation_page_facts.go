package aivision

import (
	"fmt"
	"math"
	"strconv"
	"unicode/utf8"
)

func operationFactsColumnsSQL() string {
	return ",c.model,c.provider,c.ambiguous,CAST(c.input_tokens AS CHAR) AS input_tokens,CAST(c.output_tokens AS CHAR) AS output_tokens,CAST(c.total_tokens AS CHAR) AS total_tokens,CAST(c.cache_read_input_tokens AS CHAR) AS cache_read_input_tokens,CAST(c.cache_write_input_tokens AS CHAR) AS cache_write_input_tokens,CAST(c.reasoning_tokens AS CHAR) AS reasoning_tokens,JSON_EXTRACT(p.row_json,CONCAT('$._operation_previews.',JSON_QUOTE(c.observation_id))) AS operation_preview"
}

func validateOperationPreview(preview map[string]any) error {
	if preview == nil {
		return fmt.Errorf("operation preview unavailable")
	}
	for key := range preview {
		switch key {
		case "name", "latency", "status", "input", "output", "input_truncated", "output_truncated":
		default:
			return fmt.Errorf("unsupported operation preview field")
		}
	}
	name, ok := preview["name"].(string)
	if !ok || utf8.RuneCountInString(name) > 512 {
		return fmt.Errorf("invalid operation preview name")
	}
	status, err := asInt64(preview["status"])
	if err != nil || status < 0 || status > 2 {
		return fmt.Errorf("invalid operation preview status")
	}
	latency, present := preview["latency"]
	if !present {
		return fmt.Errorf("operation preview latency unavailable")
	}
	if latency != nil {
		value, err := strconv.ParseFloat(fmt.Sprint(latency), 64)
		if err != nil || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("invalid operation preview latency")
		}
	}
	for _, field := range []string{"input", "output"} {
		value, present := preview[field]
		if !present {
			return fmt.Errorf("operation preview IO unavailable")
		}
		if value != nil {
			text, ok := value.(string)
			if !ok || utf8.RuneCountInString(text) > 1024 {
				return fmt.Errorf("invalid operation preview IO")
			}
		}
		if _, ok := preview[field+"_truncated"].(bool); !ok {
			return fmt.Errorf("operation preview truncation unavailable")
		}
	}
	return nil
}

func decodeOperationFacts(row map[string]any) (map[string]any, map[string]any, error) {
	preview, err := tracePageJSON(row["operation_preview"])
	if err != nil {
		return nil, nil, err
	}
	if err = validateOperationPreview(preview); err != nil {
		return nil, nil, err
	}
	facts := map[string]any{"model": row["model"], "provider": row["provider"]}
	for _, field := range []string{"model", "provider"} {
		if value := facts[field]; value != nil {
			text, ok := value.(string)
			if !ok || len(text) > 255 {
				return nil, nil, fmt.Errorf("invalid operation dimension")
			}
		}
	}
	ambiguous, err := asInt64(row["ambiguous"])
	if value, ok := row["ambiguous"].(bool); ok {
		ambiguous, err = int64(boolInt(value)), nil
	}
	if err != nil || ambiguous < 0 || ambiguous > 1 {
		return nil, nil, fmt.Errorf("invalid operation ambiguity")
	}
	facts["ambiguous"] = ambiguous == 1
	for _, field := range []struct{ output, column string }{{"input", "input_tokens"}, {"output", "output_tokens"}, {"total", "total_tokens"}, {"cacheReadInput", "cache_read_input_tokens"}, {"cacheWriteInput", "cache_write_input_tokens"}, {"reasoning", "reasoning_tokens"}} {
		value, present := row[field.column]
		if !present {
			return nil, nil, fmt.Errorf("operation tokens unavailable")
		}
		if value != nil {
			text, ok := value.(string)
			if !ok || !statisticsInteger.MatchString(text) {
				return nil, nil, fmt.Errorf("invalid operation token count")
			}
		}
		facts[field.output] = value
	}
	return facts, preview, nil
}
