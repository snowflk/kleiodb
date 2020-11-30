package persistence

import "time"

type SnapshotKeeper interface {
	SaveSnapshot(source, key string, data []byte) error

	GetSnapshot(source, key string) (RawSnapshot, error)

	// FindSnapshots finds all snapshots matching the given pattern from a source
	// Returns an array of snapshot keys
	FindSnapshots(source string, pattern Pattern) ([]string, error)
	Close() error
}
type RawSnapshot struct {
	Source    string
	Key       string
	Timestamp time.Time
	Payload   []byte
}
