package mysql

import (
	"fmt"
	"log"
	"strings"
)

func (s *Storage) initTables() error {
	if err := s.initStreamTbl(); err != nil {
		log.Println("error init table stream ")
		return err
	}
	if err := s.initEventTbl(); err != nil {
		log.Println("error init table event ")
		return err
	}
	if err := s.initViewTbl(); err != nil {
		log.Println("error init table view ")
		return err
	}

	if err := s.initSnapshotTbl(); err != nil {
		log.Println("error init table snapshot ")
		return err
	}
	return nil
}

func (s *Storage) initStreamTbl() error {
	_, err := s.db.Exec(`
			CREATE TABLE IF NOT EXISTS streams
			(
    			name       	VARCHAR(100) PRIMARY KEY,
    			data		BLOB 	NOT NULL,
    			version 	BIGINT NOT NULL DEFAULT 0
			);
	`)
	return err
}

func (s *Storage) initEventTbl() error {
	_, err := s.db.Exec(`
			CREATE TABLE IF NOT EXISTS raw_events
			(
				serial          BIGINT AUTO_INCREMENT PRIMARY KEY,
				data            BLOB        NOT NULL,
				created_at      TIMESTAMP NOT NULL DEFAULT NOW()
			);
	`)
	return err
}

func (s *Storage) initViewTbl() error {
	_, err := s.db.Exec(`
			CREATE TABLE IF NOT EXISTS views
			(
				view_name       VARCHAR(100)    NOT NULL,
				serial          BIGINT          NOT NULL,
				UNIQUE (view_name, serial),
				FOREIGN KEY (serial) REFERENCES raw_events(serial)
			);
	`)
	if err != nil {
		return err
	}
	return s.createIndex("views", "index_views", []string{"view_name"})
}

func (s *Storage) initSnapshotTbl() error {
	_, err := s.db.Exec(`
			CREATE TABLE IF NOT EXISTS snapshots
			(
				name 			VARCHAR(100)	NOT NULL,	
				source       	VARCHAR(100)    NOT NULL,
				data          	BLOB 			NOT NULL,
				created_at      TIMESTAMP 		NOT NULL DEFAULT NOW()
			);
	`)
	if err != nil {
		return err
	}
	return s.createIndex("snapshots", "index_snapshots", []string{"source", "name"})
}

func (s *Storage) createIndex(tableName, indexName string, columns []string) error {
	existed, err := s.isIndexExisted(tableName, indexName)
	if err != nil {
		return err
	}
	if !existed {
		_, err = s.db.Exec(fmt.Sprintf("CREATE INDEX %s ON %s(%s);", indexName, tableName, strings.Join(columns, ",")))
		return err
	}
	return nil
}

func (s *Storage) isIndexExisted(tableName, indexName string) (bool, error) {
	query := fmt.Sprintf(`
						SELECT COUNT(*)
						FROM information_schema.statistics
						WHERE TABLE_SCHEMA = DATABASE()
  							AND TABLE_NAME = '%s' 
  							AND INDEX_NAME = '%s';`, tableName, indexName)
	row := s.db.QueryRow(query)
	var counter uint64
	if row.Err() != nil {
		return false, row.Err()
	}

	if err := row.Scan(&counter); err != nil {
		return false, err
	}
	return counter >= 1, nil
}
