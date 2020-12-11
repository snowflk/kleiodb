package kleio

import "github.com/snowflk/kleiodb/internal/persistence"

type Storage struct {
	persistence.EventKeeper
	persistence.ViewKeeper
	persistence.SnapshotKeeper
}

func New(options Options) (persistence.Storage, error) {
	eventKeeper, err := NewEventKeeper(options.RootDir)
	if err != nil {
		return nil, err
	}
	kvKeeper, err := NewKVKeeper(options.RootDir)
	if err != nil {
		return nil, err
	}
	return &Storage{
		EventKeeper:    eventKeeper,
		ViewKeeper:     kvKeeper,
		SnapshotKeeper: kvKeeper,
	}, nil
}

func (s *Storage) Close() error {
	defer s.ViewKeeper.Close()
	defer s.EventKeeper.Close()
	return nil
}
