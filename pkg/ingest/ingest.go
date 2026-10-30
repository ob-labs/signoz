// Package ingest implements the signoz-side OTLP write endpoint of the
// ai-vision data plane. The sole gateway (antcollector-gw) verifies the
// ingest token, stamps the authoritative space/user identity and forwards
// OTLP/HTTP to this endpoint; the module decodes the payload and writes the
// signoz.* flat tables with the exact row contract of the retired
// gateway-side oceanbase exporter. It also owns the DDL of those tables, so
// signoz-ob is the single source of the storage schema. Read traffic never
// touches this package.
package ingest

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Config wires the ingest endpoint to the telemetry database already
// configured for the query service: the same DSN and table prefix that
// signoz-ob reads, so write and read share one storage contract.
type Config struct {
	// ProjectionMaintenanceDisabled is the single emergency ingest control.
	// The zero value enables standard revision tracking.
	ProjectionMaintenanceDisabled bool
	// ProjectionMaintenanceManaged is internal wiring: the matching data-plane
	// provider already owns initialization and bounded retry on this DSN/prefix.
	// Standalone ingest keeps its zero-value bootstrap behavior.
	ProjectionMaintenanceManaged bool
	// DSN of the OceanBase/seekdb telemetry database (MySQL protocol).
	DSN string
	// TablePrefix of the signoz.* flat tables, e.g. "signoz".
	TablePrefix string
	// OrgID is authoritative from deployment config, never from client data.
	OrgID string
	// InternalKey gates the endpoint and is shared with the gateway's otlphttp
	// exporter header. An empty key disables registration (fail closed): the
	// public API port must never accept unauthenticated telemetry writes.
	InternalKey string
	// MaxBodyBytes caps a single OTLP request body. Defaults to 32 MiB.
	MaxBodyBytes int64
	// MaxOpenConns / MaxIdleConns of the dedicated write pool.
	MaxOpenConns int
	MaxIdleConns int
}

const defaultMaxBodyBytes = 32 << 20

const defaultTablePrefix = "signoz"

// tablePrefixPattern is the allowlist the retired oceanbaseexporter enforced.
// The prefix is the only value interpolated into DDL/insert SQL (all row values
// go through placeholders), so constraining it to a safe identifier keeps the
// dynamic table name free of injection by construction.
var tablePrefixPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,31}$`)

// internalKeyHeader carries the shared gateway-to-signoz secret. It is
// distinct from the client-facing ingest token: the gateway already verified
// that token and this header only proves the caller is our gateway hop.
const internalKeyHeader = "X-Signoz-Ingest-Key"

type endpoint struct {
	cfg          Config
	db           *sql.DB
	tracesTable  string
	logsTable    string
	metricsTable string
	logger       *slog.Logger
}

// AddToRouter registers the OTLP ingest routes (/v1/traces, /v1/logs,
// /v1/metrics) on the public API router when an internal key is configured,
// and ensures the signoz.* schema exists (signoz-ob now owns the DDL). Callers
// invoke it before the web catch-all is added so the OTLP routes win over the
// frontend file server.
func AddToRouter(router *mux.Router, cfg Config, logger *slog.Logger) error {
	if cfg.InternalKey == "" {
		logger.Warn("otlp ingest endpoint disabled: internal key not configured")
		return nil
	}
	if cfg.TablePrefix == "" {
		cfg.TablePrefix = defaultTablePrefix
	}
	if !tablePrefixPattern.MatchString(cfg.TablePrefix) {
		return fmt.Errorf("ingest table_prefix %q must match %s", cfg.TablePrefix, tablePrefixPattern.String())
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = defaultMaxBodyBytes
	}
	if cfg.MaxOpenConns <= 0 {
		cfg.MaxOpenConns = 16
	}
	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = 4
	}
	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		return err
	}
	if err := ensureSchema(context.Background(), db, cfg.TablePrefix); err != nil {
		_ = db.Close()
		return err
	}
	if err := InitializeTracePageDimensions(context.Background(), db, cfg.TablePrefix); err != nil {
		logger.Warn("trace page metadata unavailable; ingestion retained", "error", err)
	}
	initCtx, initCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	if err := initializeProjectionMaintenance(initCtx, db, cfg); err != nil {
		logger.Warn("projection maintenance unavailable; raw ingestion retained with shared eligibility fence", "error", err)
	}
	if err := initializeProjectionSummaries(initCtx, db, cfg); err != nil {
		logger.Warn("projection summaries unavailable; ingestion retained", "error", err)
	}
	initCancel()
	ep := &endpoint{
		cfg:          cfg,
		db:           db,
		tracesTable:  spansTable(cfg.TablePrefix),
		logsTable:    logsTable(cfg.TablePrefix),
		metricsTable: metricsTable(cfg.TablePrefix),
		logger:       logger,
	}
	router.HandleFunc("/v1/traces", ep.handleTraces).Methods(http.MethodPost)
	router.HandleFunc("/v1/logs", ep.handleLogs).Methods(http.MethodPost)
	router.HandleFunc("/v1/metrics", ep.handleMetrics).Methods(http.MethodPost)
	logger.Info("otlp ingest endpoint registered",
		slog.String("routes", "/v1/traces,/v1/logs,/v1/metrics"),
		slog.String("table_prefix", cfg.TablePrefix))
	return nil
}

// A managed provider is the sole lifecycle owner, including failed migration
// retries. Repeating its mutating initialization here could revoke eligibility
// after a successful provider startup without scheduling a new retry.
func initializeProjectionMaintenance(ctx context.Context, db *sql.DB, cfg Config) error {
	if cfg.ProjectionMaintenanceManaged {
		return nil
	}
	return InitializeTraceReadModelMaintenance(ctx, db, cfg.TablePrefix, !cfg.ProjectionMaintenanceDisabled)
}

func initializeProjectionSummaries(ctx context.Context, db *sql.DB, cfg Config) error {
	// The managed provider already owns optional migration and retry. Repeating
	// it here both blocks route registration and competes for the same lease.
	if cfg.ProjectionMaintenanceManaged {
		return nil
	}
	return InitializeTraceProjectionSummaries(ctx, db, cfg.TablePrefix)
}

func (ep *endpoint) authorized(r *http.Request) bool {
	got := r.Header.Get(internalKeyHeader)
	if got == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(ep.cfg.InternalKey)) == 1
}

func isJSONRequest(r *http.Request) bool {
	return strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/json")
}

func readBody(w http.ResponseWriter, r *http.Request, max int64) ([]byte, error) {
	return io.ReadAll(http.MaxBytesReader(w, r.Body, max))
}

// writeOTLPResponse mirrors the OTLP/HTTP semantics: proto requests get an
// empty-body 200 (a valid zero-value Export*ServiceResponse), JSON requests get
// an empty JSON object.
func writeOTLPResponse(w http.ResponseWriter, isJSON bool) {
	if isJSON {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}"))
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
}

func (ep *endpoint) handleTraces(w http.ResponseWriter, r *http.Request) {
	if !ep.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	isJSON := isJSONRequest(r)
	body, err := readBody(w, r, ep.cfg.MaxBodyBytes)
	if err != nil {
		http.Error(w, "read otlp body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var traces ptrace.Traces
	if isJSON {
		var unmarshaler ptrace.JSONUnmarshaler
		traces, err = unmarshaler.UnmarshalTraces(body)
	} else {
		var unmarshaler ptrace.ProtoUnmarshaler
		traces, err = unmarshaler.UnmarshalTraces(body)
	}
	if err != nil {
		http.Error(w, "decode otlp traces: "+err.Error(), http.StatusBadRequest)
		return
	}
	rows, err := buildSpanRows(traces, ep.cfg.OrgID)
	if err != nil {
		http.Error(w, "map otlp traces: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := ep.insertRows(r.Context(), ep.tracesTable, spanInsertColumns, spanUpsert, rows); err != nil {
		ep.logger.Error("insert otlp traces", slog.String("err", err.Error()))
		writeStorageError(w, "insert traces", err)
		return
	}
	writeOTLPResponse(w, isJSON)
}

func (ep *endpoint) handleLogs(w http.ResponseWriter, r *http.Request) {
	if !ep.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	isJSON := isJSONRequest(r)
	body, err := readBody(w, r, ep.cfg.MaxBodyBytes)
	if err != nil {
		http.Error(w, "read otlp body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var logs plog.Logs
	if isJSON {
		var unmarshaler plog.JSONUnmarshaler
		logs, err = unmarshaler.UnmarshalLogs(body)
	} else {
		var unmarshaler plog.ProtoUnmarshaler
		logs, err = unmarshaler.UnmarshalLogs(body)
	}
	if err != nil {
		http.Error(w, "decode otlp logs: "+err.Error(), http.StatusBadRequest)
		return
	}
	rows, err := buildLogRows(logs, ep.cfg.OrgID)
	if err != nil {
		http.Error(w, "map otlp logs: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := ep.insertRows(r.Context(), ep.logsTable, logInsertColumns, logUpsert, rows); err != nil {
		ep.logger.Error("insert otlp logs", slog.String("err", err.Error()))
		writeStorageError(w, "insert logs", err)
		return
	}
	writeOTLPResponse(w, isJSON)
}

func (ep *endpoint) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if !ep.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	isJSON := isJSONRequest(r)
	body, err := readBody(w, r, ep.cfg.MaxBodyBytes)
	if err != nil {
		http.Error(w, "read otlp body: "+err.Error(), http.StatusBadRequest)
		return
	}
	var metrics pmetric.Metrics
	if isJSON {
		var unmarshaler pmetric.JSONUnmarshaler
		metrics, err = unmarshaler.UnmarshalMetrics(body)
	} else {
		var unmarshaler pmetric.ProtoUnmarshaler
		metrics, err = unmarshaler.UnmarshalMetrics(body)
	}
	if err != nil {
		http.Error(w, "decode otlp metrics: "+err.Error(), http.StatusBadRequest)
		return
	}
	rows, err := buildMetricRows(metrics, ep.cfg.OrgID)
	if err != nil {
		http.Error(w, "map otlp metrics: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := ep.insertRows(r.Context(), ep.metricsTable, metricInsertColumns, metricUpsert, rows); err != nil {
		ep.logger.Error("insert otlp metrics", slog.String("err", err.Error()))
		http.Error(w, "insert metrics: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeOTLPResponse(w, isJSON)
}

// insertRows writes pre-mapped rows in one transaction against the given table.
// The mapping is pure (build*Rows) and table/column names come from the
// allowlisted prefix, so only placeholders reach the driver here.
func (ep *endpoint) insertRows(ctx context.Context, table, columns, upsert string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := ep.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction for %s: %w", table, err)
	}
	defer func() { _ = tx.Rollback() }()
	tracking := false
	if table == ep.tracesTable {
		enabled, generation, err := LockProjectionMaintenance(ctx, tx, ep.cfg.TablePrefix)
		if err != nil {
			return fmt.Errorf("lock projection maintenance before raw write: %w", err)
		}
		tracking = enabled && !ep.cfg.ProjectionMaintenanceDisabled
		if !tracking && generation > 0 {
			query := "UPDATE " + tableName(ep.cfg.TablePrefix, "projection_maintenance") + " SET enabled=FALSE,rebuild_required=TRUE WHERE id=1"
			if ep.cfg.ProjectionMaintenanceDisabled {
				query = "UPDATE " + tableName(ep.cfg.TablePrefix, "projection_maintenance") + " SET generation=generation+IF(enabled,1,0),enabled=FALSE,suspended=TRUE,rebuild_required=TRUE WHERE id=1"
			}
			if _, err := tx.ExecContext(ctx, query); err != nil {
				return err
			}
		}
	}
	if tracking {
		for _, key := range traceSourceKeys(rows) {
			if err := LockTraceSource(ctx, tx, ep.cfg.TablePrefix, key.OrgID, key.SpaceID, key.TraceID); err != nil {
				return err
			}
		}
	}
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO "+table+columns+upsert)
	if err != nil {
		return fmt.Errorf("prepare upsert for %s: %w", table, err)
	}
	defer stmt.Close()
	for _, row := range rows {
		if _, err := stmt.ExecContext(ctx, row...); err != nil {
			return fmt.Errorf("upsert row into %s: %w", table, err)
		}
	}
	if tracking {
		if err := ep.dirtyTraceSources(ctx, tx, rows); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction for %s: %w", table, err)
	}
	return nil
}
