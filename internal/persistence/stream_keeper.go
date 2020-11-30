package persistence

type StreamKeeper interface {
	// PutStream updates a stream, or creates a stream if it does not exist.
	// The stream name should contain only digits, letters, underscore and dash symbol
	PutStream(streamName string, payload []byte) error

	// GetStream returns a stream and its metadata back
	// This low-level storage does not care about
	GetStream(streamName string) (RawStream, error)

	// FindStream finds streams using a pattern and returns their name back
	// If the pattern is invalid, the function just ignores it and returns an empty array
	FindStream(pattern Pattern) ([]string, error)

	// GetStreamVersion returns the latest version of the given stream
	// Returns an error if stream does not exist
	GetStreamVersion(streamName string) (uint32, error)

	// IncrementStreamVersion increments the latest version of the stream by a given amount
	IncrementStreamVersion(streamName string, increment uint32) (uint32, error)

	Close() error
}

type RawStream struct {
	StreamName string
	Version    uint32
	Payload    []byte
}

type Pattern string

func (p Pattern) Match(name string) bool {
	panic("implement me")
}

func (p Pattern) String() string {
	return string(p)
}
