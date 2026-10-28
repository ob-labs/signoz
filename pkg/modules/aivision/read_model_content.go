package aivision

import (
	"context"
	"database/sql"

	"github.com/SigNoz/signoz/pkg/ingest"
)

// Missing flags are legacy complete documents. This gate is part of the
// authorized, unfiltered window proof: a content predicate must never hide a
// document whose unprojected tail may contain a match.
func projectionIncompleteContentSQL(required bool, generated ...bool) string {
	if !required {
		// A bound FALSE inside CASE can still force LOB access during planning.
		// Metadata coverage must not reference row_json at all.
		return "0"
	}
	if len(generated) > 0 && generated[0] {
		return "COALESCE(SUM(CASE WHEN BINARY p.agent_uuid=BINARY ? AND p." + ingest.TraceContentCompleteColumn + "=0 THEN 1 ELSE 0 END),0)"
	}
	return "COALESCE(SUM(CASE WHEN BINARY p.agent_uuid=BINARY ? AND BINARY COALESCE(JSON_UNQUOTE(JSON_EXTRACT(p.row_json,'$._contentComplete')),'true')=BINARY 'false' THEN 1 ELSE 0 END),0)"
}

func (s *oceanBaseStore) contentCoverageCapability(ctx context.Context, tx *sql.Tx, required bool) (bool, string, error) {
	if !required {
		return false, "not_requested", nil
	}
	ready, err := ingest.TraceContentColumnReady(ctx, tx, s.cfg.TablePrefix)
	if err != nil {
		return false, "", err
	}
	if ready {
		return true, "generated_column", nil
	}
	return false, "json", nil
}

func readModelContentRequired(p nativeListParams) bool {
	return !p.MetadataOnly || p.InputKeyword != "" || p.Keyword != "" && (p.SearchType == "input" || p.SearchType == "output")
}
