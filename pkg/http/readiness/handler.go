// Package readiness provides a read-only probe of the configured telemetry store.
package readiness

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/SigNoz/signoz/pkg/telemetrystore"
)

// NewHandler serves anonymous database readiness with a one-second deadline.
// It uses the portable query API so both OceanBase and ClickHouse are supported.
// Only fixed status values are returned; connection errors can contain secrets.
func NewHandler(store telemetrystore.TelemetryStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()

		ready := false
		if store != nil {
			rows, err := store.QueryContext(ctx, "SELECT 1")
			if err == nil && rows != nil {
				var value uint8
				ready = rows.Next() && rows.Scan(&value) == nil && value == 1 && !rows.Next() && rows.Err() == nil
				if rows.Close() != nil {
					ready = false
				}
			}
		}
		if ctx.Err() != nil {
			ready = false
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		status := "ok"
		if !ready {
			status = "unavailable"
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"status": status})
	}
}
