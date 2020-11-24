package postgres

type PostgresStorage struct {
}

func (s *PostgresStorage) CreateStream(streamName string) error {
	panic("implement me")
}

func (s *PostgresStorage) Append(streamName string, data []interface{}) ([]uint64, error) {
	panic("implement me")
}

func (s *PostgresStorage) AddToIndex(sequenceNumbers []uint64, streamName, indexName string) error {
	panic("implement me")
}

func (s *PostgresStorage) Get(streamName string, offset, limit uint64) ([]interface{}, error) {
	panic("implement me")
}

func (s *PostgresStorage) GetByIndex(streamName, indexName string, offset, limit uint64) ([]interface{}, error) {
	panic("implement me")
}
