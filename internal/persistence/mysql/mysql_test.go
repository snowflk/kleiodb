package mysql

import (
	"fmt"
	"github.com/snowflk/kleiodb/internal/persistence"
	"github.com/snowflk/kleiodb/internal/persistence/testsuite"
	"github.com/stretchr/testify/suite"
	"log"
	"testing"
)

func TestMySQL(t *testing.T) {
	suite.Run(t, testsuite.NewTestSuite(provider))
}

var provider = func() persistence.Storage {
	storage, err := New(Options{"localhost", 3306, "user", "example", "mysql"})
	if err != nil {
		log.Fatal("FAILED to create", err)
	}
	if err = storage.dropAllIndex(); err != nil {
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

func (s *Storage) dropIndex(tableName, indexName string) error {
	existed, err := s.isIndexExisted(tableName, indexName)
	if err != nil {
		return nil
	}
	if existed {
		_, err := s.db.Exec(fmt.Sprintf("DROP INDEX %s ON %s", indexName, tableName))
		return err
	}
	return nil
}

func (s *Storage) dropAllIndex() error {
	if err := s.dropIndex("snapshots", "index_snapshots"); err != nil {
		return err
	}

	if err := s.dropIndex("views", "index_views"); err != nil {
		return err
	}
	return nil
}
