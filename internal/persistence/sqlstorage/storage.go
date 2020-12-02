package sqlstorage

import (
	"errors"
	"fmt"
	p "github.com/snowflk/kleiodb/internal/persistence"
	"log"
	"regexp"
	"strings"
)

const MaxOpenConnections = 20
const MaxIdleConnections = 0

var (
	ErrViewEmpty            = errors.New("view name cannot empty")
	ErrStreamNotExist       = errors.New("stream does not exist")
	ErrStreamEmpty          = errors.New("stream name cannot empty")
	ErrStreamInvalid        = errors.New("stream name should contain only digits, letters, underscore and dash symbol")
	ErrDataInvalid          = errors.New("data is invalid")
	ErrDataEmpty            = errors.New("data cannot empty")
	ErrSnapshotNameEmpty    = errors.New("snapshot's name cannot empty")
	ErrSnapshotSourceEmpty  = errors.New("snapshot's source cannot empty")
	ErrSnapshotPatternEmpty = errors.New("pattern cannot empty")
)

type storage struct {
	db dbConn
}

type Options struct {
	Driver   string
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

func New(options Options) (*storage, error) {
	var db dbConn
	var err error

	if options.Driver == "postgres" {
		db, err = newPostgresConnection(options)
	} else if options.Driver == "mysql" {
		db, err = newMySQLConnection(options)
	}

	return &storage{db: db}, err
}

func (s *storage) CreateViewMeta(viewName string, payload []byte) error {
	if err := validateViewName(viewName); err != nil {
		return err
	}
	return s.db.exec("INSERT INTO streams(name, data) VALUES(?,?);", viewName, payload)
}

func (s *storage) GetViewMeta(viewName string) (p.RawView, error) {
	var rawStream p.RawView
	if err := validateViewName(viewName); err != nil {
		return rawStream, err
	}
	row := s.db.queryOne("SELECT name, version, data FROM streams WHERE name = ?;", viewName)
	if err := row.Scan(&rawStream.ViewName, &rawStream.Version, &rawStream.Payload); err != nil {
		return rawStream, err
	}
	return rawStream, nil
}

func (s *storage) FindViews(pattern p.Pattern) ([]string, error) {
	rows, err := s.db.query("SELECT name FROM streams WHERE name like ?;", pattern.String())
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

func (s *storage) GetViewVersion(viewName string) (uint32, error) {
	if err := validateViewName(viewName); err != nil {
		return 0, err
	}
	row := s.db.queryOne(`
							SELECT version 
							FROM streams 
							WHERE name = ?
							ORDER BY version DESC LIMIT 1;`, viewName)
	var version uint32 = 0
	if err := row.Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func (s *storage) IncrementViewVersion(streamName string, increment uint32) (uint32, error) {
	if err := validateViewName(streamName); err != nil {
		return 0, err
	}
	return s.db.incrementViewVersion(streamName, `UPDATE streams
							SET version = version + ?
							WHERE name = ?`, increment, streamName)
}

func (s *storage) UpdateViewMeta(viewName string, payload []byte) error {
	if err := validateViewName(viewName); err != nil {
		return err
	}
	if payload == nil {
		return ErrDataInvalid
	}
	row := s.db.queryOne("SELECT COUNT(*) FROM streams WHERE name = ?;", viewName)
	var counter = 0
	if err := row.Scan(&counter); err != nil {
		return err
	}
	if counter == 0 {
		return ErrStreamNotExist
	}
	return s.db.exec(`UPDATE streams
						SET data = ? 
						WHERE name = ?;`, payload, viewName)

}

func (s *storage) AppendEvents(batchData [][]byte, views []string) ([]uint64, error) {
	if batchData == nil || len(batchData) == 0 {
		return nil, ErrDataInvalid
	}
	dataSize := len(batchData)
	// Prepare query and arguments
	queryPlaceholders := make([]string, dataSize)
	queryArgs := make([]interface{}, dataSize)
	for i, data := range batchData {
		queryPlaceholders[i] = "(?)"
		queryArgs[i] = data
	}

	serialNumbers, err := s.db.appendEvents(uint16(dataSize), fmt.Sprintf("INSERT INTO raw_events (data) VALUES %s ",
		strings.Join(queryPlaceholders, ",")), queryArgs...)
	if err != nil {
		log.Println("ERR", err)
		return nil, err
	}

	if views != nil {
		// Append serials to views based on view name
		queryPlaceholders := make([]string, dataSize*len(views))
		queryArgs := make([]interface{}, 2*dataSize*len(views))
		for i, view := range views {
			for j, serial := range serialNumbers {
				idx := i*len(serialNumbers) + j
				queryPlaceholders[idx] = "(?, ?)"
				queryArgs[idx*2] = view
				queryArgs[idx*2+1] = serial
			}
		}
		err = s.db.exec(
			fmt.Sprintf("INSERT INTO views (view_name, serial) VALUES %s",
				strings.Join(queryPlaceholders, ",")), queryArgs...)
		if err != nil {
			return nil, err
		}
	}
	return serialNumbers, nil
}

func (s *storage) AddEventsToView(viewName string, serials []uint64) error {
	if checkNameEmpty(viewName) {
		return ErrViewEmpty
	}
	if serials == nil {
		return ErrDataInvalid
	}
	if len(serials) == 0 {
		return ErrDataEmpty
	}
	queryPlaceholders := make([]string, len(serials))
	queryArgs := make([]interface{}, 2*len(serials))
	for i, serial := range serials {
		queryPlaceholders[i] = "(?,?)"
		queryArgs[2*i] = viewName
		queryArgs[2*i+1] = serial
	}
	return s.db.exec(
		fmt.Sprintf("INSERT INTO views(view_name, serial) VALUES %s",
			strings.Join(queryPlaceholders, ",")), queryArgs...)
}

func (s *storage) GetEventsFromView(viewName string, offset, limit uint64) ([]p.RawEvent, error) {
	if checkNameEmpty(viewName) {
		return nil, ErrViewEmpty
	}
	query := `
			SELECT e.serial, e.data, e.created_at
			FROM views v
			INNER JOIN raw_events e ON  e.serial = v.serial
			WHERE view_name = ?`
	args := []interface{}{viewName}
	return s.queryRawEvents(query, args, offset, limit)
}

func (s *storage) GetEvents(offset, limit uint64) ([]p.RawEvent, error) {
	return s.queryRawEvents(`
				SELECT serial, data, created_at FROM raw_events`,
		[]interface{}{}, offset, limit)
}

func (s *storage) SaveSnapshot(source, key string, data []byte) error {
	if checkNameEmpty(source) {
		return ErrSnapshotSourceEmpty
	}

	if checkNameEmpty(key) {
		return ErrSnapshotNameEmpty
	}

	return s.db.exec("INSERT INTO snapshots(name, source, data) VALUES(?, ?, ?);", key, source, data)
}

func (s *storage) GetSnapshot(source, key string) (p.RawSnapshot, error) {
	var snapShot p.RawSnapshot

	if checkNameEmpty(source) {
		return snapShot, ErrSnapshotSourceEmpty
	}

	if checkNameEmpty(key) {
		return snapShot, ErrSnapshotNameEmpty
	}
	rows, err := s.db.query("SELECT source, name, data, created_at FROM snapshots WHERE source = ? AND name = ? LIMIT 1;", source, key)
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

func (s *storage) FindSnapshot(source string, pattern p.Pattern) ([]string, error) {
	if checkNameEmpty(source) {
		return nil, ErrSnapshotSourceEmpty
	}

	if checkNameEmpty(pattern.String()) {
		return nil, ErrSnapshotNameEmpty
	}

	rows, err := s.db.query("SELECT DISTINCT name FROM snapshots WHERE source = ? AND name like ?;", source, pattern.String())
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

func (s *storage) Close() error {
	return s.db.close()
}

func (s *storage) init() error {
	if err := s.db.initStreamTbl(); err != nil {
		return err
	}
	if err := s.db.initEventTbl(); err != nil {
		return err
	}
	if err := s.db.initViewTbl(); err != nil {
		return err
	}
	return nil
}

func validateViewName(viewName string) error {
	if len(viewName) == 0 {
		return ErrStreamEmpty
	}
	isViewName := regexp.MustCompile(`^[A-Za-z0-9\-\_]+$`).MatchString
	if !isViewName(viewName) {
		return ErrStreamInvalid
	}
	return nil
}

func addOffsetLimit(q string, args []interface{}, offset, limit uint64) (string, []interface{}) {
	q += " LIMIT ?"
	args = append(args, limit)
	q += " OFFSET ?"
	args = append(args, offset)
	return q, args
}

func (s *storage) queryRawEvents(query string, args []interface{}, offset, limit uint64) ([]p.RawEvent, error) {
	query, args = addOffsetLimit(query, args, offset, limit)

	rows, err := s.db.query(query, args...)
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

func (s *storage) checkStreamExistence(streamName string) error {
	row := s.db.queryOne("SELECT COUNT(*) FROM streams WHERE name = ?;", streamName)
	var counter int8
	if err := row.Scan(&counter); err != nil {
		return err
	}
	if counter == 0 {
		return ErrStreamNotExist
	}
	return nil
}

func checkNameEmpty(name string) bool {
	name = strings.TrimSpace(name)
	return len(name) == 0
}
