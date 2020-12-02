package sqlstorage

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

type mysqlConn struct {
	conn *sql.DB
}

func (c *mysqlConn) incrementViewVersion(viewName, q string, args ...interface{}) (uint32, error) {
	tx, err := c.conn.Begin()
	if err != nil {
		return 0, err
	}

	_, err = tx.Exec(q, args...)
	if err != nil {
		return 0, err
	}

	row := tx.QueryRow("SELECT version FROM streams WHERE name = ?;", viewName)

	var version uint32
	if err := row.Scan(&version); err != nil {
		tx.Rollback()
		return version, err
	}
	if err = tx.Commit(); err != nil {
		return version, err
	}
	return version, nil

}

func (c *mysqlConn) appendEvents(nEvents uint16, q string, args ...interface{}) ([]uint64, error) {
	tx, err := c.conn.Begin()
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(q, args...)
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(`SELECT serial
					FROM (SELECT serial FROM raw_events ORDER BY serial DESC LIMIT ?)s
					ORDER BY serial ASC`, nEvents)
	defer rows.Close()
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	serials := make([]uint64, nEvents)
	counter := 0
	for rows.Next() {
		if err = rows.Scan(&serials[counter]); err != nil {
			return nil, err
		}
		counter++
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return serials, nil

}

func (c *mysqlConn) initStreamTbl() error {
	return c.exec(`
			CREATE TABLE IF NOT EXISTS streams
			(
    			name       	VARCHAR(100) PRIMARY KEY,
    			data		BLOB 	NOT NULL,
    			version 	BIGINT NOT NULL DEFAULT 0
			);
	`)
}

func (c *mysqlConn) initEventTbl() error {
	return c.exec(`
			CREATE TABLE IF NOT EXISTS raw_events
			(
				serial          BIGINT AUTO_INCREMENT PRIMARY KEY,
				data            BLOB        NOT NULL,
				created_at      TIMESTAMP NOT NULL DEFAULT NOW()
			);
	`)
}

func (c *mysqlConn) initViewTbl() error {
	err := c.exec(`
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
	return c.createIndex("views", "index_views", []string{"view_name"})
}

func (c *mysqlConn) initSnapshotTbl() error {
	err := c.exec(`
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
	return c.createIndex("snapshots", "index_snapshots", []string{"source", "name"})
}

func (c *mysqlConn) dropAllIndex() error {
	if err := c.dropIndex("snapshots", "index_snapshots"); err != nil {
		return err
	}

	if err := c.dropIndex("views", "index_views"); err != nil {
		return err
	}
	return nil
}

func (c *mysqlConn) query(q string, args ...interface{}) (*sql.Rows, error) {
	return c.conn.Query(q, args...)
}

func (c *mysqlConn) queryOne(q string, args ...interface{}) *sql.Row {
	return c.conn.QueryRow(q, args...)
}
func (c *mysqlConn) exec(q string, args ...interface{}) error {
	err := c.conn.QueryRow(q, args...).Scan()
	if err == sql.ErrNoRows {
		return nil
	}
	return err
}

func (c *mysqlConn) close() error {
	return c.conn.Close()
}

func (c *mysqlConn) createIndex(tableName, indexName string, columns []string) error {
	existed, err := c.isIndexExisted(tableName, indexName)
	if err != nil {
		return err
	}
	if !existed {
		return c.exec(fmt.Sprintf("CREATE INDEX %s ON %s(%s);", indexName, tableName, strings.Join(columns, ",")))
	}
	return nil
}

func (c *mysqlConn) isIndexExisted(tableName, indexName string) (bool, error) {
	query := fmt.Sprintf(`
						SELECT COUNT(*)
						FROM information_schema.statistics
						WHERE TABLE_SCHEMA = DATABASE()
  							AND TABLE_NAME = '%s' 
  							AND INDEX_NAME = '%s';`, tableName, indexName)
	row := c.queryOne(query)
	var counter uint64
	if row.Err() != nil {
		return false, row.Err()
	}

	if err := row.Scan(&counter); err != nil {
		return false, err
	}
	return counter >= 1, nil
}

func (c *mysqlConn) dropIndex(tableName, indexName string) error {
	existed, err := c.isIndexExisted(tableName, indexName)
	if err != nil {
		return nil
	}
	if existed {
		return c.exec(fmt.Sprintf("DROP INDEX %s ON %s", indexName, tableName))
	}
	return nil
}

func newMySQLConnection(opts Options) (dbConn, error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		opts.User, opts.Password, opts.Host, opts.Port, opts.Database)
	db, err := sql.Open("mysql", connStr)
	db.SetMaxOpenConns(MaxOpenConnections)
	db.SetMaxIdleConns(MaxIdleConnections)
	return &mysqlConn{conn: db}, err
}
