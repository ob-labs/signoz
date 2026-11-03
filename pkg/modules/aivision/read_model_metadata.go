package aivision

import (
	"github.com/SigNoz/signoz/pkg/ingest"
	"strings"
)

// Retained for projection-equivalence tests. Query paths read the versioned
// STORED columns after capability validation, rather than evaluating this SQL.
func readModelRowJSONSQL(metadataOnly bool) string {
	expression := ingest.TraceProjectionContentExpression
	if metadataOnly {
		expression = ingest.TraceProjectionMetadataExpression
	}
	return strings.ReplaceAll(expression, "row_json", "p.row_json")
}

func readModelStoredRowJSONSQL(metadataOnly bool) string {
	if metadataOnly {
		return "p." + ingest.TraceProjectionMetadataColumn
	}
	return "p." + ingest.TraceProjectionContentColumn
}
