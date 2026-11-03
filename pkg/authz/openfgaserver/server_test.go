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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	providerSettings := instrumentationtest.New().ToProviderSettings()

	sqlstore := sqlstoretest.New(sqlstore.Config{Provider: "sqlite"}, sqlmock.QueryMatcherRegexp)
	canCloseSQL := true
	t.Cleanup(func() {
		// Never mutate sqlmock expectations while Start may still be using them.
		if !canCloseSQL {
			t.Error("retaining SQL mock because provider Start did not exit")
			return
		}
		sqlstore.Mock().ExpectClose()
		require.NoError(t, sqlstore.SQLDB().Close())
	})

	openfgaDataStore, err := NewSQLStore(sqlstore, authz.Config{OpenFGA: authz.OpenFGAConfig{MaxTuplesPerWrite: 100}})
	require.NoError(t, err)

	expectedModel := `module base 
	type user`
	provider, err := NewOpenfgaServer(context.Background(), providerSettings, authz.Config{}, sqlstore, []transformer.ModuleFile{{Name: "test.fga", Contents: expectedModel}}, openfgaDataStore)
	require.NoError(t, err)

	storeRows := sqlstore.Mock().NewRows([]string{"id", "name", "created_at", "updated_at"}).AddRow("01K3V0NTN47MPTMEV1PD5ST6ZC", "signoz", time.Now(), time.Now())
	sqlstore.Mock().ExpectQuery("SELECT (.+) FROM store WHERE (.+)").WillReturnRows(storeRows)

	authModelRows := sqlstore.Mock().NewRows([]string{"authorization_model_id", "schema_version", "serialized_protobuf"}).
		AddRow("01K44QQKXR6F729W160NFCJT58", "1.1", []byte(""))
	sqlstore.Mock().ExpectQuery("SELECT (.+) FROM authorization_model WHERE (.+)").WillReturnRows(authModelRows)

	sqlstore.Mock().ExpectExec("INSERT INTO authorization_model (.+) VALUES (.+)").WillReturnResult(sqlmock.NewResult(1, 1))

	startResult := make(chan error, 1)
	startDone := make(chan struct{})
	canCloseSQL = false
	// Registered after SQL cleanup so cancellation, Stop and the join run first,
	// including when a startup assertion fails.
	t.Cleanup(func() {
		cancel()
		if err := provider.Stop(context.Background()); err != nil {
			t.Errorf("stop provider during cleanup: %v", err)
		}
		select {
		case <-startDone:
			canCloseSQL = true
		case <-time.After(30 * time.Second):
			t.Error("provider Start did not exit during cleanup")
		}
	})
	go func() {
		defer close(startDone)
		startResult <- provider.Start(ctx)
	}()

	select {
	case <-provider.Healthy():
	case err := <-startResult:
		t.Fatalf("provider Start returned before becoming healthy: %v", err)
	case <-ctx.Done():
		t.Fatal("provider did not become healthy before the safety deadline")
	}

	require.NoError(t, provider.Stop(context.Background()))
	select {
	case <-startDone:
		require.NoError(t, <-startResult)
	case <-time.After(30 * time.Second):
		t.Fatal("provider Start did not exit after Stop")
	}
}
