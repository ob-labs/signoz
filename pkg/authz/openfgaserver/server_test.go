package openfgaserver

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/SigNoz/signoz/pkg/authz"
	"github.com/SigNoz/signoz/pkg/instrumentation/instrumentationtest"
	"github.com/SigNoz/signoz/pkg/sqlstore"
	"github.com/SigNoz/signoz/pkg/sqlstore/sqlstoretest"
	"github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
)

func TestProviderStartStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	providerSettings := instrumentationtest.New().ToProviderSettings()
	sqlstore := sqlstoretest.New(sqlstore.Config{Provider: "sqlite"}, sqlmock.QueryMatcherRegexp)
	t.Cleanup(func() { require.NoError(t, sqlstore.BunDB().Close()) })

	openfgaDataStore, err := NewSQLStore(sqlstore, authz.Config{OpenFGA: authz.OpenFGAConfig{MaxTuplesPerWrite: 100}})
	require.NoError(t, err)

	expectedModel := `module base 
	type user`
	provider, err := NewOpenfgaServer(context.Background(), providerSettings, authz.Config{}, sqlstore, []transformer.ModuleFile{{Name: "test.fga", Contents: expectedModel}}, openfgaDataStore)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Stop(ctx)) })

	storeRows := sqlstore.Mock().NewRows([]string{"id", "name", "created_at", "updated_at"}).AddRow("01K3V0NTN47MPTMEV1PD5ST6ZC", "signoz", time.Now(), time.Now())
	sqlstore.Mock().ExpectQuery("SELECT (.+) FROM store WHERE (.+)").WillReturnRows(storeRows)

	authModelRows := sqlstore.Mock().NewRows([]string{"authorization_model_id", "schema_version", "serialized_protobuf"}).
		AddRow("01K44QQKXR6F729W160NFCJT58", "1.1", []byte(""))
	sqlstore.Mock().ExpectQuery("SELECT (.+) FROM authorization_model WHERE (.+)").WillReturnRows(authModelRows)

	sqlstore.Mock().ExpectExec("INSERT INTO authorization_model (.+) VALUES (.+)").WillReturnResult(sqlmock.NewResult(1, 1))
	sqlstore.Mock().ExpectClose()

	started := make(chan error, 1)
	go func() {
		started <- provider.Start(ctx)
	}()
	select {
	case <-provider.Healthy():
	case err := <-started:
		t.Fatalf("server failed to start: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	err = provider.Stop(ctx)
	require.NoError(t, err)
	select {
	case err := <-started:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, sqlstore.SQLDB().PingContext(ctx))
	require.NoError(t, sqlstore.BunDB().Close())
	require.NoError(t, sqlstore.Mock().ExpectationsWereMet())
}
