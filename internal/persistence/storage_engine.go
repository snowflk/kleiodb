package persistence

// StorageEngine is the interface to low level storage
// All storages must implement this interface in order to work with Kleio
// This storage only cares about streams and indices.
// Each streams can contains events as byte arrays. In other words, a stream is
//  nothing more than a collection of byte arrays
// An index contains references to the events (their sequence number), so that
// we can query the events selectively
type StorageEngine interface {
	// Create a stream, this only marks the stream as "exist"
	// An event can only be appended when the target stream exists
	// The stream name must be unique, otherwise an error will be returned
	// The stream name should contain only digits, letters, underscore and dash symbol
	CreateStream(streamName string) error

	// Append a list of events to the stream, each event is a byte array.
	// Therefore, the parameter "data" must be a two-dimensional array (array of byte array)
	// The stream must exist, or an error will be returned.
	// Returns an array of sequence numbers of appended events
	Append(streamName string, data []bytearray) ([]uint64, error)

	// Add the events to an index, so that they can be queried faster in future.
	// If the index does not exist, it will create a new one.
	// Parameters:
	// - sequenceNumbers: array of event sequence numbers
	// - indexName: name of index, this must contain only digits, letters and underscore
	AddToIndex(sequenceNumbers []uint64, streamName, indexName string) error

	// Get events directly from a given stream
	// Offset and Limit are analogous to SQL
	// This return an array of byte array, each corresponds to an event
	Get(streamName string, offset, limit uint64) ([]bytearray, error)

	// Get events from a given stream but only indexed ones
	// If the stream or the index does not exists, an error will be returned
	// Offset and Limit are analogous to SQL
	GetByIndex(streamName, indexName string, offset, limit uint64) ([]bytearray, error)
}

// bytearray is just for readability
type bytearray []byte
