package mysql

import (
	"database/sql"
	"fmt"
	"github.com/snowflk/kleiodb/internal/persistence"
)

const (
	MaxIdleConnections = 0
	MaxOpenConnections = 20
)

type Storage struct {
	db *sql.DB
}

type Options struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

func New(opts Options) (*Storage, error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		opts.User, opts.Password, opts.Host, opts.Port, opts.Database)
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(MaxOpenConnections)
	db.SetMaxIdleConns(MaxIdleConnections)
	return &Storage{db: db}, nil
}

func (s *Storage) AppendEvents(data [][]byte, views []string) ([]uint64, error) {
	panic("implement me")
}

func (s *Storage) AddEventsToView(viewName string, serials []uint64) error {
	panic("implement me")
}

func (s *Storage) GetEventsFromView(viewName string, offset, limit uint64) ([]persistence.RawEvent, error) {
	panic("implement me")
}

func (s *Storage) ClearView(viewName string) error {
	panic("implement me")
}

func (s *Storage) GetEvents(offset, limit uint64) ([]persistence.RawEvent, error) {
	panic("implement me")
}

func (s *Storage) CreateViewMeta(viewName string, payload []byte) error {
	panic("implement me")
}

func (s *Storage) GetViewMeta(viewName string) (persistence.RawView, error) {
	panic("implement me")
}

func (s *Storage) FindViews(pattern persistence.SearchPattern) ([]string, error) {
	panic("implement me")
}

func (s *Storage) GetViewVersion(viewName string) (uint32, error) {
	panic("implement me")
}

func (s *Storage) IncrementViewVersion(viewName string, increment uint32) (uint32, error) {
	panic("implement me")
}

func (s *Storage) UpdateViewMeta(viewName string, payload []byte) error {
	panic("implement me")
}

func (s *Storage) SaveSnapshot(source, name string, data []byte) error {
	panic("implement me")
}

func (s *Storage) GetSnapshot(source, name string) (persistence.RawSnapshot, error) {
	panic("implement me")
}

func (s *Storage) FindSnapshots(source string, pattern persistence.SearchPattern) ([]string, error) {
	panic("implement me")
}

func (s *Storage) Close() error {
	panic("implement me")
}
