package persistence

import "time"

// SnapshotKeeper is a storage for snapshots
// A "source" is a stream or a view the snapshot is based on
// A snapshot belongs to a single source and has a unique name
type SnapshotKeeper interface {
	// SaveSnapshot saves snapshot that belongs to the given source.
	// If the name already exists, the data will be overwritten
	SaveSnapshot(source, name string, data []byte) error

	// GetSnapshot retrieve the snapshot from the given source
	// If the source or the name cannot be found, an error will be returned
	GetSnapshot(source, name string) (RawSnapshot, error)

	// FindSnapshots finds all snapshots matching the given pattern from a source
	// Returns an array of snapshot keys
	FindSnapshots(source string, pattern Pattern) ([]string, error)
	Close() error
}

// RawSnapshot contains metadata and payload of the snapshot
type RawSnapshot struct {
	Source    string
	Name      string
	Timestamp time.Time
	Payload   []byte
}
