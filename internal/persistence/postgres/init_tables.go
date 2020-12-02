package postgres

func (s *Storage) initTables() error {
	if err := s.initStreamTbl(); err != nil {
		return err
	}
	if err := s.initEventTbl(); err != nil {
		return err
	}
	if err := s.initViewTbl(); err != nil {
		return err
	}
	return nil
}

func (s *Storage) initStreamTbl() error {
	_, err := s.db.Exec(`
			CREATE TABLE IF NOT EXISTS streams
			(
    			name       	VARCHAR(100) PRIMARY KEY,
    			data		BYTEA 	NOT NULL,
    			version 	INT NOT NULL DEFAULT 0
			);
	`)
	return err
}

func (s *Storage) initEventTbl() error {
	_, err := s.db.Exec(`
			CREATE TABLE IF NOT EXISTS raw_events
			(
				serial          BIGSERIAL PRIMARY KEY,
				data            BYTEA        NOT NULL,
				created_at      TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc')
			);
	`)
	return err
}

func (s *Storage) initViewTbl() error {
	_, err := s.db.Exec(`
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

	_, err = s.db.Exec(`CREATE INDEX IF NOT EXISTS index_views ON views(view_name);`)
	return err
}

func (s *Storage) initSnapshotTbl() error {
	_, err := s.db.Exec(`
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

	_, err = s.db.Exec(`CREATE INDEX IF NOT EXISTS index_snapshots ON snapshots(source, name);`)
	return err
}
