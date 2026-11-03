package aivision

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

const traceSpanSourceByteLimit = 16 * 1024 * 1024
const traceWindowSourceByteLimit = 512 * 1024 * 1024

// All variable-width fields selected by nativeSpanProjection participate,
// including status_message. Numeric values and JSON framing fit the additional
// per-row allowance. Raw source bytes and escaped response bytes have separate
// limits; rows are streamed and checked after encoding before accumulating.
func nativeSpanSourceBytesSQL() string {
	columns := []string{"trace_id", "span_id", "parent_span_id", "span_name", "span_kind", "status_message", "service_name", "user_id", "space_id", "agent_product", "session_id", "resource_attributes", "attributes", "events", "links", "payload"}
	parts := []string{}
	for _, column := range columns {
		parts = append(parts, "OCTET_LENGTH(COALESCE("+column+",''))")
	}
	return "(" + strings.Join(parts, "+") + "+2048)"
}

func scanReadModelRowsEach(ctx context.Context, tx *sql.Tx, query string, args []any, consume func(map[string]any) error) error {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	for rows.Next() {
		values := make([]any, len(columns))
		refs := make([]any, len(columns))
		for i := range values {
			refs[i] = &values[i]
		}
		if err := rows.Scan(refs...); err != nil {
			return err
		}
		row := map[string]any{}
		for i, column := range columns {
			value := values[i]
			if b, ok := value.([]byte); ok {
				value = string(b)
			}
			row[column] = value
		}
		if err := consume(row); err != nil {
			return err
		}
	}
	return rows.Err()
}

// Match project_observation's merge exactly. Span columns override payload
// fields, while payload attributes are overlaid by the normalized attributes.
// Only shadowed copies are removed. Payload-only fallback fields remain under
// payload, preserving the authorization reader's distinction from raw columns.
func compactNativeSpan(row map[string]any) error {
	payload, ok := row["payload"].(map[string]any)
	if !ok {
		return nil
	}
	merged := map[string]any{}
	if value, exists := payload["attributes"]; exists && value != nil {
		attrs, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid payload attributes object")
		}
		for key, value := range attrs {
			merged[key] = value
		}
	}
	if value, exists := row["attributes"]; exists && value != nil {
		attrs, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid normalized attributes object")
		}
		for key, value := range attrs {
			merged[key] = value
		}
	}
	row["attributes"] = merged
	for key := range row {
		if key != "payload" {
			delete(payload, key)
		}
	}
	delete(row, "resource_attributes") // resource already carries exactly this map.
	if len(payload) == 0 {
		delete(row, "payload")
	}
	return nil
}

func encodedSpanBytes(row map[string]any) (int, error) {
	encoded, err := json.Marshal(row)
	if err != nil {
		return 0, err
	}
	return len(encoded) + 128, nil
}
