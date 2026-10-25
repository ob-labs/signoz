package ingest

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestProjectionBootstrapManagedProviderRemainsSoleOwner(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		cfg := Config{TablePrefix: "test", ProjectionMaintenanceManaged: true, ProjectionMaintenanceDisabled: disabled}
		// No SQL is expected: even a metadata failure in a redundant initializer
		// could otherwise revoke the already healthy provider's eligibility.
		if err := initializeProjectionMaintenance(context.Background(), db, cfg); err != nil {
			t.Fatalf("managed provider must not repeat initializer: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
}

func TestProjectionBootstrapStandaloneStillInitializes(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectHealthyMaintenanceInitialization(mock)
	if err := initializeProjectionMaintenance(context.Background(), db, Config{TablePrefix: "test"}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProjectionBootstrapStandaloneFailureRemainsVisible(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	probeErr := errors.New("temporary control schema failure")
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `test_projection_maintenance`").WillReturnError(probeErr)
	if err := initializeProjectionMaintenance(context.Background(), db, Config{TablePrefix: "test"}); !errors.Is(err, probeErr) {
		t.Fatalf("standalone bootstrap must preserve error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
