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
		return &borrowedSQLiteDatastore{Datastore: datastore}, nil
	}
	return nil, errors.Newf(errors.TypeInvalidInput, errors.CodeInvalidInput, "invalid store type: %s", store.BunDB().Dialect().Name().String())
}

// borrowedSQLiteDatastore leaves the platform-owned pool available to other
// services, including their final shutdown writes. The platform closes it only
// after all consumers have stopped.
type borrowedSQLiteDatastore struct {
	*sqlite.Datastore
}

// Close deliberately does not call sqlite.Datastore.Close: in OpenFGA v1.14.1
// that method only unregisters its optional DB stats collector and closes the
// pool. ExportMetrics must remain false above, so there is no datastore-owned
// resource to release here. Revisit this adapter when upgrading OpenFGA or
// enabling datastore metrics. OpenFGA Server.Close still releases its own caches,
// planner and throttlers. Repeated calls to this borrowed-store Close are safe.
func (*borrowedSQLiteDatastore) Close() {}
