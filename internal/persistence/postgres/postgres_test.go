package postgres

import (
	"github.com/snowflk/kleiodb/internal/persistence"
	"github.com/snowflk/kleiodb/internal/persistence/testsuite"
	"github.com/stretchr/testify/suite"
	"log"
	"testing"
)

func TestPostgres(t *testing.T) {
	suite.Run(t, testsuite.NewTestSuite(provider))
}

var provider = func() persistence.Storage {
	storage, err := New(Options{"localhost", 5432, "postgres", "example", "postgres"})
	if err != nil {
		log.Fatal("FAILED to create", err)
	}
	_, err = storage.db.Exec("DROP INDEX IF EXISTS index_snapshots;")
	if err != nil {
		panic(err)
	}

	_, err = storage.db.Exec("DROP INDEX IF EXISTS index_views;")
	if err != nil {
		panic(err)
	}

	_, err = storage.db.Exec("DROP TABLE IF EXISTS snapshots CASCADE;")
	if err != nil {
		panic(err)
	}

	_, err = storage.db.Exec("DROP TABLE IF EXISTS views CASCADE;")
	if err != nil {
		panic(err)
	}

	_, err = storage.db.Exec("DROP TABLE IF EXISTS raw_events CASCADE;")
	if err != nil {
		panic(err)
	}

	_, err = storage.db.Exec("DROP TABLE IF EXISTS streams CASCADE;")
	if err != nil {
		panic(err)
	}

	err = storage.initTables()

	if err != nil {
		panic(err)
	}

	return storage
}
