package sqlstorage

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"strings"
)

type postgresConn struct {
	conn *sql.DB
}

func (c *postgresConn) initStreamTbl() error {
	return c.exec(`
			CREATE TABLE IF NOT EXISTS streams
			(
    			name       	VARCHAR(100) PRIMARY KEY,
    			data		BYTEA 	NOT NULL,
    			version 	INT NOT NULL DEFAULT 0
			);
	`)
}

func (c *postgresConn) initEventTbl() error {
	return c.exec(`
			CREATE TABLE IF NOT EXISTS raw_events
			(
				serial          BIGSERIAL PRIMARY KEY,
				data            BYTEA        NOT NULL,
				created_at      TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc')
			);
	`)
}

func (c *postgresConn) initViewTbl() error {
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

	return c.exec(`CREATE INDEX IF NOT EXISTS index_views ON views(view_name);`)
}

func (c *postgresConn) initSnapshotTbl() error {
	err := c.exec(`
			CREATE TABLE IF NOT EXISTS snapshots
			(
				name 			VARCHAR(100)	NOT NULL,	
				source       	VARCHAR(100)    NOT NULL,
				data          	BYTEA 			NOT NULL,
				created_at      TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc')
			);
	`)
	if err != nil {
		return err
	}

	return c.exec(`CREATE INDEX IF NOT EXISTS index_snapshots ON snapshots(source, name);`)
}

func (c *postgresConn) dropAllIndex() error {
	err := c.exec("DROP INDEX IF EXISTS index_snapshots;")
	if err != nil {
		panic(err)
	}

	err = c.exec("DROP INDEX IF EXISTS index_views;")
	if err != nil {
		panic(err)
	}
	return nil
}

func (c *postgresConn) query(q string, args ...interface{}) (*sql.Rows, error) {
	return c.conn.Query(makeQuery(q), args...)
}

func (c *postgresConn) queryOne(q string, args ...interface{}) *sql.Row {
	return c.conn.QueryRow(makeQuery(q), args...)
}

func (c *postgresConn) exec(q string, args ...interface{}) error {
	err := c.conn.QueryRow(makeQuery(q), args...).Scan()
	if err == sql.ErrNoRows {
		return nil
	}
	return err
}

func (c *postgresConn) close() error {
	return c.conn.Close()
}

func newPostgresConnection(opts Options) (dbConn, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		opts.Host, opts.Port, opts.User, opts.Password, opts.Database)
	db, err := sql.Open("postgres", connStr)
	db.SetMaxOpenConns(MaxOpenConnections)
	db.SetMaxIdleConns(MaxIdleConnections)

	return &postgresConn{conn: db}, err
}

func makeQuery(q string) string {
	counter := 1
	for i := strings.Index(q, "?"); i >= 0; i = strings.Index(q, "?") {
		q = strings.Replace(q, "?", fmt.Sprintf("$%d", counter), 1)
		counter++
	}
	return q
}
