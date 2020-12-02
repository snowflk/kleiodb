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
	// AppendEvents a list of events to the stream
	// Each event is a byte array, therefore, the parameter "data" must be a two-dimensional array (array of byte array)
	// The parameter views is a list containing the view keys that those events should be indexed
	// Returns an array of serials of appended events
	AppendEvents(data [][]byte, views []string) ([]uint64, error)

	// AddEventsToView the events to an index, so that they can be queried faster in future.
	// If the index does not exist, it will create a new one.
	// This operation should be idempotent
	// Parameters:
	// - viewName: name of the custom view, this must contain only digits, letters and underscore
	// - serials: array of serial
	AddEventsToView(viewName string, serials []uint64) error

	GetEventsFromView(viewName string, offset, limit uint64) ([]RawEvent, error)

	// DeleteView deletes a view.
	// If the view does not exist, an error will be returned
	ClearView(viewName string) error

	// Get events from the global stream
	// Offset and Limit are analogous to SQL
	GetEvents(offset, limit uint64) ([]RawEvent, error)

	// Close closes the connection
	Close() error
}

type RawEvent struct {
	Serial    uint64
	Payload   []byte
	Timestamp time.Time
}
