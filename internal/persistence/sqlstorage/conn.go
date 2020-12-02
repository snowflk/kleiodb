package sqlstorage

import "database/sql"

type dbConn interface {
	initStreamTbl() error
	initEventTbl() error
	initViewTbl() error
	initSnapshotTbl() error
	dropAllIndex() error
	query(q string, args ...interface{}) (*sql.Rows, error)
	queryOne(q string, args ...interface{}) *sql.Row
	appendEvents(nEvents uint16, q string, args ...interface{}) ([]uint64, error)
	incrementViewVersion(viewName, q string, args ...interface{}) (uint32, error)
	exec(q string, args ...interface{}) error
	close() error
}
