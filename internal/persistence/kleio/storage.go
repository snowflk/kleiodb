package kleio

import (
	log "github.com/sirupsen/logrus"
	"github.com/snowflk/kleiodb/internal/persistence"
)

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
	log.Info("Created")
	return &Storage{
		EventKeeper:    eventKeeper,
		ViewKeeper:     kvKeeper,
		SnapshotKeeper: kvKeeper,
	}, nil
}

func (s *Storage) Close() error {
	defer s.EventKeeper.Close()
	return s.ViewKeeper.Close()
}

func (s *Storage) ForceFlush() error {
	return nil
}
