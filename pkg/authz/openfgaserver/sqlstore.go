package openfgaserver

import (
	"github.com/SigNoz/signoz/pkg/authz"
	"github.com/SigNoz/signoz/pkg/errors"
	"github.com/SigNoz/signoz/pkg/sqlstore"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/openfga/openfga/pkg/storage/sqlite"
)

func NewSQLStore(store sqlstore.SQLStore, config authz.Config) (storage.OpenFGADatastore, error) {
	switch store.BunDB().Dialect().Name().String() {
	case "sqlite":
		datastore, err := sqlite.NewWithDB(store.SQLDB(), &sqlcommon.Config{
			MaxTuplesPerWriteField: config.OpenFGA.MaxTuplesPerWrite,
			MaxTypesPerModelField:  100,
			ExportMetrics:          false,
		})
		if err != nil {
			return nil, err
		}
		return &borrowedSQLDatastore{OpenFGADatastore: datastore}, nil

	}
	return nil, errors.Newf(errors.TypeInvalidInput, errors.CodeInvalidInput, "invalid store type: %s", store.BunDB().Dialect().Name().String())
}

// borrowedSQLDatastore leaves the platform SQLStore responsible for its pool.
// OpenFGA v1.14.1 SQLite Close only unregisters its optional DBStatsCollector and
// closes the supplied pool. ExportMetrics is disabled above, so there are no
// datastore-owned resources to release. Recheck that contract on dependency
// upgrades; the full OpenFGA server still releases its own caches and workers.
type borrowedSQLDatastore struct {
	storage.OpenFGADatastore
}

func (*borrowedSQLDatastore) Close() {}
