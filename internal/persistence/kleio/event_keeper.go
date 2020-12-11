package kleio

import (
	"github.com/pkg/errors"
	"github.com/snowflk/kleiodb/internal/persistence"
	"os"
	"sync"
	"time"
)

const (
	StreamSysVersion    = 1
	globalStreamName    = "data"
	globalStreamFileFmt = "%s.kdb"
)

type EventKeeper struct {
	mu      sync.RWMutex
	rootDir string

	// gStream is the global stream storing all events in the system
	gStream *GlobalStream

	// indexManager takes care of reading/writing indices
	indexManager *IndexManager
}

type Options struct {
	RootDir string
}

// NewEventKeeper creates a new instance of EventKeeper, and therefore, creates a new global stream if it does not exist
// in the given root directory
func NewEventKeeper(rootDir string) (*EventKeeper, error) {
	if err := os.MkdirAll(rootDir, 0744|os.ModeSticky); err != nil {
		return nil, errors.Wrap(err, "failed to open data directory")
	}
	globalStream, err := OpenGlobalStream("data", GlobalStreamOpts{RootDir: rootDir})
	if err != nil {
		return nil, err
	}
	indexManager := NewIndexManager(rootDir)
	kfs := &EventKeeper{
		rootDir:      rootDir,
		gStream:      globalStream,
		indexManager: indexManager,
	}
	return kfs, nil
}

// Append appends multiple events into the global stream
// Note: You should only pass the views where strong consistency is required.
// Too many views to index may result to slow overall performance
func (k *EventKeeper) AppendEvents(data [][]byte, views []string) ([]uint64, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	serials, positions, err := k.gStream.Append(data)
	if err != nil {
		return nil, err
	}
	if err := k.indexManager.AppendGlobalIndex(positions); err != nil {
		return nil, err
	}
	for _, viewName := range views {
		if err := k.indexManager.AppendViewIndex(viewName, positions); err != nil {
			return nil, err
		}
	}
	return serials, nil
}

// AddEventsToView adds the events with given serials into an index
// The main usage of this is to build an index from scratch ( when index file is corrupted or when custom index is needed)
// You can use pagination with Get using offset and limit, find the matching events and use AddToView to perform indexing
// It is recommended to performs this operation in chunks (for example: only 1000 events at once)
func (k *EventKeeper) AddEventsToView(viewName string, serials []uint64) error {
	if len(serials) == 0 {
		return nil
	}
	minSerial := serials[0]
	maxSerial := serials[0]
	for _, serial := range serials {
		if serial < minSerial {
			minSerial = serial
		}
		if serial > maxSerial {
			maxSerial = serial
		}
	}
	positions, err := k.indexManager.GetGlobal(minSerial-1, maxSerial-minSerial+1)
	if err != nil {
		return err
	}
	return k.indexManager.AppendViewIndex(viewName, positions)
}

func (k *EventKeeper) GetEventsFromView(viewName string, offset, limit uint64) ([]persistence.RawEvent, error) {
	positions, err := k.indexManager.GetByView(viewName, offset, limit)
	if err != nil {
		return nil, err
	}
	return k.get(positions)
}

func (k *EventKeeper) GetEvents(offset, limit uint64) ([]persistence.RawEvent, error) {
	positions, err := k.indexManager.GetGlobal(offset, limit)
	if err != nil {
		return nil, err
	}
	return k.get(positions)
}

func (k *EventKeeper) ClearView(viewName string) error {
	return k.indexManager.ClearView(viewName)
}

func (k *EventKeeper) get(positions []int64) ([]persistence.RawEvent, error) {
	entries, err := k.gStream.ReadByIndex(positions)
	if err != nil {
		return nil, err
	}
	events := make([]persistence.RawEvent, len(entries))
	for i, entry := range entries {
		events[i] = persistence.RawEvent{
			Serial:    entry.Serial,
			Payload:   entry.Payload,
			Timestamp: time.Unix(entry.Timestamp, 0),
		}
	}
	return events, nil
}

// rebuildGlobalIndex should only be called on initialization
// If the global index is corrupted for any reason (for example: sudden shutdown while writing)
func (k *EventKeeper) rebuildGlobalIndex(fromSerial uint64) error {
	panic("implement me")
}

func (k *EventKeeper) Close() error {
	defer k.indexManager.Shutdown()
	defer k.gStream.Close()
	return nil
}
