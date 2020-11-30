package persistence

import (
	"time"
)

// StorageEngine is the interface to low level storage
// All storages must implement this interface in order to work with Kleio
// This storage only cares about streams and indices.
// Each streams can contains events as byte arrays. In other words, a stream is
//  nothing more than a collection of byte arrays
// An index contains references to the events (their sequence number), so that
// we can query the events selectively
type EventKeeper interface {
	// Append a list of events to the stream
	// Each event is a byte array, therefore, the parameter "data" must be a two-dimensional array (array of byte array)
	// Returns an array of serials of appended events
	Append(data [][]byte) ([]uint64, error)

	// Add the events to an index, so that they can be queried faster in future.
	// If the index does not exist, it will create a new one.
	// Parameters:
	// - viewName: name of the custom view, this must contain only digits, letters and underscore
	// - serials: array of serial
	AddToView(viewName string, serials []uint64) error

	GetFromView(viewName string, offset, limit uint64) ([]RawEvent, error)
	// Get events from the global stream
	// Offset and Limit are analogous to SQL
	Get(offset, limit uint64) ([]RawEvent, error)

	// Close closes the connection
	Close() error
}

type RawEvent struct {
	Serial    uint64
	Payload   []byte
	Timestamp time.Time
}
