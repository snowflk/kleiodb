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
				created_at      TIMESTAMP
			);
	`)
}

func (c *mysqlConn) initViewTbl() error {
	err := c.exec(`
			CREATE TABLE IF NOT EXISTS views
			(
				view_name       VARCHAR(100)    NOT NULL,
				serial          BIGINT          NOT NULL REFERENCES raw_events(serial),
				UNIQUE (view_name, serial)
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
				created_at      TIMESTAMP
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
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		opts.User, opts.Password, opts.Host, opts.Port, opts.Database)
	db, err := sql.Open("mysql", connStr)
	db.SetMaxOpenConns(MaxOpenConnections)
	db.SetMaxIdleConns(MaxIdleConnections)
	return &mysqlConn{conn: db}, err
}
