package oceanbasetelemetrystore

import (
	"context"
	"database/sql"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/SigNoz/signoz/pkg/errors"
	"github.com/SigNoz/signoz/pkg/factory"
	"github.com/SigNoz/signoz/pkg/ingest"
	"github.com/SigNoz/signoz/pkg/telemetrystore"
	"github.com/SigNoz/signoz/pkg/types/telemetrystoretypes"
	"github.com/go-sql-driver/mysql"
)

type provider struct {
	db                *sql.DB
	config            telemetrystore.OceanBaseConfig
	hooks             []telemetrystore.TelemetryStoreHook
	maintenanceCancel context.CancelFunc
}

var _ telemetrystore.TelemetryStore = (*provider)(nil)

func NewFactory(hooks ...factory.ProviderFactory[telemetrystore.TelemetryStoreHook, telemetrystore.Config]) factory.ProviderFactory[telemetrystore.TelemetryStore, telemetrystore.Config] {
	return factory.NewProviderFactory(factory.MustNewName("oceanbase"), func(ctx context.Context, settings factory.ProviderSettings, config telemetrystore.Config) (telemetrystore.TelemetryStore, error) {
		return New(ctx, settings, config, hooks...)
	})
}

func New(ctx context.Context, settings factory.ProviderSettings, config telemetrystore.Config, hookFactories ...factory.ProviderFactory[telemetrystore.TelemetryStoreHook, telemetrystore.Config]) (telemetrystore.TelemetryStore, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	dsn, err := mysql.ParseDSN(config.OceanBase.DSN)
	if err != nil {
		return nil, errors.NewInvalidInputf(errors.CodeInvalidInput, "invalid OceanBase DSN")
	}
	// Timestamp result types have the same UTC semantics as the querier. Keep
	// prepared parameters and forbid multiple statements, including in custom DSNs.
	dsn.ParseTime = true
	dsn.Loc = time.UTC
	dsn.MultiStatements = false
	dsn.InterpolateParams = false
	dsn.Timeout = config.Connection.DialTimeout
	connector, err := mysql.NewConnector(dsn)
	if err != nil {
		return nil, errors.NewInvalidInputf(errors.CodeInvalidInput, "invalid OceanBase connection configuration")
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(config.Connection.MaxOpenConns)
	db.SetMaxIdleConns(config.Connection.MaxIdleConns)
	db.SetConnMaxLifetime(5 * time.Minute)
	hooks := make([]telemetrystore.TelemetryStoreHook, 0, len(hookFactories))
	for _, f := range hookFactories {
		hook, err := f.New(ctx, settings, config)
		if err != nil {
			db.Close()
			return nil, err
		}
		hooks = append(hooks, hook)
	}
	pingCtx, cancel := context.WithTimeout(ctx, config.Connection.DialTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, errors.WrapInternalf(err, errors.CodeInternal, "connect to OceanBase telemetry store")
	}
	// Projection migration is bounded independently from short query deadlines.
	// A derived-schema failure must not stop the raw telemetry service. The
	// durable migration fence remains ineligible until retry finishes.
	initCtx, initCancel := context.WithTimeout(ctx, 2*time.Minute)
	complete, err := initializeProjectionModels(initCtx, db, config.OceanBase.TablePrefix)
	initCancel()
	if err != nil && settings.Logger != nil {
		settings.Logger.Warn("projection maintenance initialization failed; raw queries remain available", "error", err)
	}

	result := &provider{db: db, config: config.OceanBase, hooks: hooks}
	if err != nil || !complete {
		retryCtx, retryCancel := context.WithCancel(ctx)
		result.maintenanceCancel = retryCancel
		go retryProjectionMaintenance(retryCtx, 5*time.Second, time.Minute, func(attemptCtx context.Context) (bool, error) {
			complete, attemptErr := initializeProjectionModels(attemptCtx, db, config.OceanBase.TablePrefix)
			if attemptErr != nil && retryCtx.Err() == nil && settings.Logger != nil {
				settings.Logger.Warn("retry projection maintenance initialization", "error", attemptErr)
			}
			return complete, attemptErr
		})
	}
	return result, nil
}

func initializeProjectionModels(ctx context.Context, db *sql.DB, prefix string) (bool, error) {
	enabled := ingest.ProjectionMaintenanceEnabled()
	if err := ingest.InitializeTraceReadModelMaintenance(ctx, db, prefix, enabled); err != nil {
		return false, err
	}
	if !enabled {
		return true, nil
	}
	// OceanBase 4.4+ uses grouping sets; recognized seekdb 1.4+ uses a compatible
	// grouping-axis query over the same optional typed storage. Older/unknown
	// engines retain the complete metrics-window contract.
	if err := ingest.InitializeTraceStatistics(ctx, db, prefix); err != nil {
		return false, err
	}
	// Keep the existing statistics initializer independent of this optional
	// helper. Content failure retains JSON coverage and a pending retry; it must
	// not prevent otherwise supported statistics from becoming usable.
	if err := ingest.InitializeTraceContentColumn(ctx, db, prefix); err != nil {
		return false, err
	}
	// A nil initializer result can also mean durable OFF. Keep the provider's
	// worker pending until supported optional storage is actually ready, so a
	// later explicit resume can recover from a transient DDL failure by itself.
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return false, err
	}
	if !ingest.TraceStatisticsVersionSupported(version) {
		return true, nil
	}
	return ingest.TraceStatisticsSchemaReady(ctx, db, prefix)
}

func (p *provider) QueryContext(ctx context.Context, query string, args ...any) (telemetrystore.Rows, error) {
	event := telemetrystore.NewQueryEvent(query, args)
	ctx = telemetrystore.WrapBeforeQuery(p.hooks, ctx, event)
	ctx, cancel := context.WithTimeout(ctx, p.config.QueryTimeout)
	finish := func(err error) {
		event.Err = err
		telemetrystore.WrapAfterQuery(p.hooks, ctx, event)
		cancel()
	}
	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		finish(err)
		return nil, err
	}
	result, err := newRows(rows, event.StartTime, finish)
	if err != nil {
		rows.Close()
		finish(err)
		return nil, err
	}
	result.maxRows = p.config.MaxResultRows
	return result, nil
}

func (p *provider) Close() error {
	if p.maintenanceCancel != nil {
		p.maintenanceCancel()
	}
	return p.db.Close()
}
func (p *provider) Ping(ctx context.Context) error { return p.db.PingContext(ctx) }
func (p *provider) ClickhouseDB() clickhouse.Conn  { return telemetrystore.UnsupportedClickHouse{} }
func (p *provider) Cluster() string                { return "" }
func (p *provider) Estimate(context.Context, string, ...any) ([]telemetrystoretypes.EstimateEntry, error) {
	return nil, telemetrystore.Unsupported("ClickHouse scan estimates")
}
func (p *provider) Plan(ctx context.Context, stmt string, args ...any) error {
	rows, err := p.QueryContext(ctx, "EXPLAIN "+stmt, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
	}
	return rows.Err()
}
func (p *provider) Indexes(context.Context, string, ...any) (telemetrystoretypes.Granules, bool, error) {
	return telemetrystoretypes.Granules{}, false, telemetrystore.Unsupported("ClickHouse granule indexes")
}

// SQLDB shares the existing pool with domain operations requiring a transaction.
func (p *provider) SQLDB() *sql.DB { return p.db }

// Retry transient migration failures without taking the raw service offline.
// Nil pending results idle without warnings or optional DDL while durable OFF.
// Shutdown cancels both a pending delay and an active bounded attempt.
func retryProjectionMaintenance(ctx context.Context, retryInterval, idleInterval time.Duration, initialize func(context.Context) (bool, error)) {
	delay := retryInterval
	for {
		if ctx.Err() != nil {
			return
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if ctx.Err() != nil {
			return
		}
		attemptCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		complete, err := initialize(attemptCtx)
		cancel()
		if err == nil && complete {
			return
		}
		delay = retryInterval
		if err == nil {
			delay = idleInterval
		}
	}
}
