package postgres

import (
	"database/sql"
	"fmt"
	p "github.com/snowflk/kleiodb/internal/persistence"
	"log"
	"strings"
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
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		opts.Host, opts.Port, opts.User, opts.Password, opts.Database)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(MaxOpenConnections)
	db.SetMaxIdleConns(MaxIdleConnections)
	return &Storage{
		db: db,
	}, nil
}

func (s *Storage) AppendEvents(data [][]byte, views []string) ([]uint64, error) {
	if data == nil || len(data) == 0 {
		return nil, p.ErrDataInvalid
	}
	dataSize := len(data)
	// Prepare query and arguments
	queryPlaceholders := make([]string, dataSize)
	queryArgs := make([]interface{}, dataSize)
	for i, item := range data {
		queryPlaceholders[i] = fmt.Sprintf("$%d", i+1)
		queryArgs[i] = item
	}

	rows, err := s.db.Query(fmt.Sprintf("INSERT INTO raw_events (data) VALUES %s RETURNING serial",
		strings.Join(queryPlaceholders, ",")), queryArgs...)
	defer rows.Close()
	if err != nil {
		log.Println("ERR", err)
		return nil, err
	}

	serialNumbers := make([]uint64, dataSize)
	counter := 0
	for rows.Next() {
		if err = rows.Scan(&serialNumbers[counter]); err != nil {
			return nil, err
		}
		counter++
	}

	if views != nil {
		// Append serials to views based on view name
		queryPlaceholders := make([]string, dataSize*len(views))
		queryArgs := make([]interface{}, 2*dataSize*len(views))
		for i, view := range views {
			for j, serial := range serialNumbers {
				idx := i*len(serialNumbers) + j
				queryPlaceholders[idx] = fmt.Sprintf("($%d, $%d)", idx*2+1, idx*2+2)
				queryArgs[idx*2] = view
				queryArgs[idx*2+1] = serial
			}
		}
		_, err = s.db.Exec(
			fmt.Sprintf("INSERT INTO views (view_name, serial) VALUES %s",
				strings.Join(queryPlaceholders, ",")), queryArgs...)
		if err != nil {
			return nil, err
		}
	}
	return serialNumbers, nil
}

func (s *Storage) AddEventsToView(viewName string, serials []uint64) error {
	if p.CheckStringEmpty(viewName) {
		return p.ErrViewEmpty
	}
	if serials == nil {
		return p.ErrDataInvalid
	}
	if len(serials) == 0 {
		return p.ErrDataEmpty
	}
	queryPlaceholders := make([]string, len(serials))
	queryArgs := make([]interface{}, 2*len(serials))
	for i, serial := range serials {
		queryPlaceholders[i] = fmt.Sprintf("($%d,$%d)", 2*i+1, 2*i+2)
		queryArgs[2*i] = viewName
		queryArgs[2*i+1] = serial
	}
	_, err := s.db.Exec(
		fmt.Sprintf("INSERT INTO views(view_name, serial) VALUES %s",
			strings.Join(queryPlaceholders, ",")), queryArgs...)
	return err
}

func (s *Storage) GetEventsFromView(viewName string, offset, limit uint64) ([]p.RawEvent, error) {
	if p.CheckStringEmpty(viewName) {
		return nil, p.ErrViewEmpty
	}
	query := `
			SELECT e.serial, e.data, e.created_at
			FROM views v
			INNER JOIN raw_events e ON  e.serial = v.serial
			WHERE view_name = $1`
	args := []interface{}{viewName}
	return s.queryRawEvents(query, args, offset, limit)
}

func (s *Storage) ClearView(viewName string) error {
	_, err := s.db.Exec("DELETE FROM views WHERE view_name = $1;", viewName)
	return err
}

func (s *Storage) GetEvents(offset, limit uint64) ([]p.RawEvent, error) {
	return s.queryRawEvents(`
				SELECT serial, data, created_at FROM raw_events`,
		[]interface{}{}, offset, limit)
}

func (s *Storage) CreateViewMeta(viewName string, payload []byte) error {
	if err := p.ValidateString(viewName); err != nil {
		return err
	}
	_, err := s.db.Exec("INSERT INTO streams(name, data) VALUES($1, $2);", viewName, payload)
	return err
}

func (s *Storage) GetViewMeta(viewName string) (p.RawView, error) {
	var rawStream p.RawView
	if err := p.ValidateString(viewName); err != nil {
		return rawStream, err
	}
	row := s.db.QueryRow("SELECT name, version, data FROM streams WHERE name = $1;", viewName)
	if err := row.Scan(&rawStream.ViewName, &rawStream.Version, &rawStream.Payload); err != nil {
		return rawStream, err
	}
	return rawStream, nil
}

func (s *Storage) FindViews(pattern p.SearchPattern) ([]string, error) {
	rows, err := s.db.Query("SELECT name FROM streams WHERE name like $1;", pattern.String())
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	viewNames := make([]string, 0)
	for rows.Next() {
		var streamName string
		if err = rows.Scan(&streamName); err != nil {
			return nil, err
		}
		viewNames = append(viewNames, streamName)
	}
	return viewNames, nil
}

func (s *Storage) GetViewVersion(viewName string) (uint32, error) {
	if err := p.ValidateString(viewName); err != nil {
		return 0, err
	}
	row := s.db.QueryRow(`
							SELECT version 
							FROM streams 
							WHERE name = $1
							ORDER BY version DESC LIMIT 1;`, viewName)
	var version uint32 = 0
	if err := row.Scan(&version); err != nil {
		return version, err
	}
	return version, nil
}

func (s *Storage) IncrementViewVersion(viewName string, increment uint32) (uint32, error) {
	if err := p.ValidateString(viewName); err != nil {
		return 0, err
	}
	row := s.db.QueryRow(`UPDATE streams
							SET version = version + $1
							WHERE name = $2 
							RETURNING version;`, increment, viewName)
	var version uint32

	if err := row.Scan(&version); err != nil {
		return version, err
	}
	return version, nil
}

func (s *Storage) UpdateViewMeta(viewName string, payload []byte) error {
	if err := p.ValidateString(viewName); err != nil {
		return err
	}
	if payload == nil {
		return p.ErrDataInvalid
	}
	row := s.db.QueryRow(`UPDATE streams
						SET data = $1 
						WHERE name = $2
						RETURNING name;`, payload, viewName)
	var result string
	if err := row.Scan(&result); err != nil {
		return p.ErrStreamNotExist
	}
	return nil
}

func (s *Storage) SaveSnapshot(source, name string, data []byte) error {
	err := p.ValidateSnapshotSourceAndName(source, name)
	if err != nil {
		return err
	}

	_, err = s.db.Exec("INSERT INTO snapshots(name, source, data) VALUES($1, $2, $3);", name, source, data)
	return err
}

func (s *Storage) GetSnapshot(source, name string) (p.RawSnapshot, error) {
	var snapShot p.RawSnapshot

	err := p.ValidateSnapshotSourceAndName(source, name)
	if err != nil {
		return snapShot, err
	}

	rows, err := s.db.Query("SELECT source, name, data, created_at FROM snapshots WHERE source = $1 AND name = $2 LIMIT 1;", source, name)
	defer rows.Close()
	if err != nil {
		return snapShot, err
	}

	for rows.Next() {
		if err = rows.Scan(&snapShot.Source, &snapShot.Name, &snapShot.Payload, &snapShot.Timestamp); err != nil {
			return snapShot, err
		}
	}

	return snapShot, nil
}

func (s *Storage) FindSnapshots(source string, pattern p.SearchPattern) ([]string, error) {
	if p.CheckStringEmpty(source) {
		return nil, p.ErrSnapshotSourceEmpty
	}

	rows, err := s.db.Query("SELECT DISTINCT name FROM snapshots WHERE source = $1 AND name like $2;", source, pattern.String())
	defer rows.Close()
	if err != nil {
		return []string{}, err
	}

	names := make([]string, 0)

	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}

	return names, nil
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func (s *Storage) queryRawEvents(query string, args []interface{}, offset, limit uint64) ([]p.RawEvent, error) {
	query, args = addOffsetLimit(query, args, offset, limit)

	rows, err := s.db.Query(query, args...)
	defer rows.Close()
	if err != nil {
		log.Println("ERR", err.Error())
		return nil, err
	}

	rawEvents := make([]p.RawEvent, 0)
	for rows.Next() {
		var rawEvent p.RawEvent
		if err := rows.Scan(&rawEvent.Serial, &rawEvent.Payload, &rawEvent.Timestamp); err != nil {
			return nil, err
		}
		rawEvents = append(rawEvents, rawEvent)
	}
	return rawEvents, nil
}

func addOffsetLimit(q string, args []interface{}, offset, limit uint64) (string, []interface{}) {
	q += fmt.Sprintf(" OFFSET $%d", len(args)+1)
	args = append(args, offset)
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT $%d", len(args)+2)
		args = append(args, limit)
	}
	return q, args
}
