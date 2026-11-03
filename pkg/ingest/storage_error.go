package ingest

import (
	"errors"
	"net/http"

	"github.com/go-sql-driver/mysql"
)

// Only classify explicit database lock failures. insertRows rolls its whole
// transaction back before returning a statement failure, so retrying the OTLP
// batch cannot acknowledge a partial write. Generic deadlines, lost connections
// and other transport errors can have an unknown commit outcome and remain 500.
func retryableStorageError(err error) bool {
	var databaseError *mysql.MySQLError
	return errors.As(err, &databaseError) && (databaseError.Number == 1205 || databaseError.Number == 1213)
}

func writeStorageError(w http.ResponseWriter, operation string, err error) {
	status := http.StatusInternalServerError
	if retryableStorageError(err) {
		// OTLP/HTTP retries 429, 502, 503 and 504, not arbitrary 5xx.
		// https://opentelemetry.io/docs/specs/otlp/#retryable-response-codes
		status = http.StatusServiceUnavailable
		w.Header().Set("Retry-After", "1")
	}
	http.Error(w, operation+": "+err.Error(), status)
}
