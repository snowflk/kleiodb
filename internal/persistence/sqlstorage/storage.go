package sqlstorage

import (
	"errors"
	"fmt"
	p "github.com/snowflk/kleiodb/internal/persistence"
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

func (s *storage) CreateStream(streamName string, payload []byte) error {
	if err := validateStreamName(streamName); err != nil {
		return err
	}
	return s.db.exec("INSERT INTO streams(name, data) VALUES(?,?);", streamName, payload)
}

func (s *storage) GetStream(streamName string) (p.RawStream, error) {
	var rawStream p.RawStream
	if err := validateStreamName(streamName); err != nil {
		return rawStream, err
	}
	row := s.db.queryOne("SELECT name, version, data FROM streams WHERE name = ?;", streamName)
	if err := row.Scan(&rawStream.StreamName, &rawStream.Version, &rawStream.Payload); err != nil {
		return rawStream, err
	}
	return rawStream, nil
}

func (s *storage) FindStream(pattern p.Pattern) ([]string, error) {
	rows, err := s.db.query("SELECT name FROM streams WHERE name like ?;", pattern.String())
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	streamNames := make([]string, 0)
	for rows.Next() {
		var streamName string
		if err = rows.Scan(&streamName); err != nil {
			return nil, err
		}
		streamNames = append(streamNames, streamName)
	}
	return streamNames, nil
}

func (s *storage) GetStreamVersion(streamName string) (uint32, error) {
	if err := validateStreamName(streamName); err != nil {
		return 0, err
	}
	row := s.db.queryOne(`
							SELECT version 
							FROM streams 
							WHERE name = ?
							ORDER BY version DESC LIMIT 1;`, streamName)
	var version uint32 = 0
	if err := row.Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func (s *storage) IncrementStreamVersion(streamName string, increment uint32) (uint32, error) {
	if err := validateStreamName(streamName); err != nil {
		return 0, err
	}

	row := s.db.queryOne(`UPDATE streams
						SET version = version + ? 
						WHERE name = ?
						RETURNING version;`, increment, streamName)
	var version uint32 = 0
	if err := row.Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func (s *storage) UpdateStreamPayload(streamName string, payload []byte) error {
	if err := validateStreamName(streamName); err != nil {
		return err
	}
	if payload == nil {
		return ErrDataInvalid
	}
	row := s.db.queryOne(`UPDATE streams
						SET data = ? 
						WHERE name = ?
						RETURNING name;`, payload, streamName)
	var result string
	if err := row.Scan(&result); err != nil {
		return err
	}
	return nil
}

func (s *storage) AppendEvents(batchData [][]byte, views []string) ([]uint64, error) {
	if batchData == nil {
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

	rows, err := s.db.query(
		fmt.Sprintf("INSERT INTO raw_events (data) VALUES %s RETURNING serial",
			strings.Join(queryPlaceholders, ",")), queryArgs...)
	defer rows.Close()
	if err != nil {
		return nil, err
	}

	// Retrieve sequence numbers back
	serialNumbers := make([]uint64, dataSize)
	i := 0
	for rows.Next() {
		if err := rows.Scan(&serialNumbers[i]); err != nil {
			return nil, err
		}
		i++
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
		rows, err = s.db.query(
			fmt.Sprintf("INSERT INTO views (view_name, serial) VALUES %s",
				strings.Join(queryPlaceholders, ",")), queryArgs...)
		defer rows.Close()
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
	query := `SELECT serial, data, created_at 
				FROM raw_events`
	return s.queryRawEvents(query, []interface{}{}, offset, limit)
}

func (s *storage) SaveSnapshot(source, key string, data []byte) error {
	if checkNameEmpty(source) {
		return ErrSnapshotSourceEmpty
	}

	if checkNameEmpty(key) {
		return ErrSnapshotNameEmpty
	}

	row := s.db.queryOne("INSERT INTO snapshots(name, source, data) VALUES(?, ?, ?) RETURNING name", key, source, data)
	if err := row.Scan(&key); err != nil {
		return err
	}
	return nil
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

func validateStreamName(streamName string) error {
	if len(streamName) == 0 {
		return ErrStreamEmpty
	}
	isStreamName := regexp.MustCompile(`^[A-Za-z0-9\-\_]+$`).MatchString
	if !isStreamName(streamName) {
		return ErrStreamInvalid
	}
	return nil
}

func addOffsetLimit(q string, args []interface{}, offset, limit uint64) (string, []interface{}) {
	q += " OFFSET ?"
	args = append(args, offset)
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	return q, args
}

func (s *storage) queryRawEvents(query string, args []interface{}, offset, limit uint64) ([]p.RawEvent, error) {
	query, args = addOffsetLimit(query, args, offset, limit)

	rows, err := s.db.query(query, args...)
	defer rows.Close()
	if err != nil {
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
