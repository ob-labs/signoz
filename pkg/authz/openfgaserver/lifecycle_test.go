package openfgaserver

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/SigNoz/signoz/pkg/authz"
	"github.com/SigNoz/signoz/pkg/instrumentation/instrumentationtest"
	"github.com/openfga/language/pkg/go/transformer"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/stretchr/testify/require"
)

type countedDatastore struct {
	storage.OpenFGADatastore
	closes atomic.Int32
}

func (store *countedDatastore) Close() {
	store.closes.Add(1)
	store.OpenFGADatastore.Close()
}

func TestServerRejectsMissingDatastore(t *testing.T) {
	server, err := NewOpenfgaServer(context.Background(), instrumentationtest.New().ToProviderSettings(), authz.Config{}, nil, nil, nil)
	require.Error(t, err)
	require.Nil(t, server)
}

func TestServerFailedStartReleasesResourcesWithoutClosingSharedPool(t *testing.T) {
	for _, stage := range []string{"store", "model"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			store := realSharedSQLStore(t)
			if stage == "model" {
				// Store initialization succeeds, but the model tables are missing.
				_, err := store.SQLDB().ExecContext(ctx, `CREATE TABLE store (
					id TEXT PRIMARY KEY, name TEXT, created_at DATETIME,
					updated_at DATETIME, deleted_at DATETIME)`)
				require.NoError(t, err)
			}
			datastore, err := NewSQLStore(store, authz.Config{})
			require.NoError(t, err)
			counted := &countedDatastore{OpenFGADatastore: datastore}
			server, err := NewOpenfgaServer(ctx, instrumentationtest.New().ToProviderSettings(), authz.Config{}, store,
				[]transformer.ModuleFile{{Name: "test.fga", Contents: "module base\ntype user"}}, counted)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.Stop(ctx)) })

			err = server.Start(ctx)
			require.Error(t, err)
			require.EqualValues(t, 1, counted.closes.Load(), "failed initialization must release OpenFGA resources")
			select {
			case <-server.Healthy():
				t.Fatal("failed server must not become healthy")
			default:
			}
			require.NoError(t, server.Stop(ctx))
			require.EqualValues(t, 1, counted.closes.Load())
			require.NoError(t, store.SQLDB().PingContext(ctx))
		})
	}
}
