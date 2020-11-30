package persistence

type StreamKeeper interface {
	// Create creates a stream.
	// The stream name should contain only digits, letters, underscore and dash symbol
	// Returns error if the stream name already existed
	Create(streamName string, payload []byte) error

	// Get returns a stream and its metadata back
	Get(streamName string) (RawStream, error)

	// Find finds streams using a pattern and returns their name back
	// If the pattern is invalid, the function just ignores it and returns an empty array
	Find(pattern Pattern) ([]string, error)

	// GetVersion returns the latest version of the given stream
	// Returns an error if stream does not exist
	GetVersion(streamName string) (uint32, error)

	// IncrementVersion increments the latest version of the stream by a given amount
	IncrementVersion(streamName string, increment uint32) (uint32, error)

	// UpdatePayload updates payload of an existed stream
	// Return error if the stream name does not exist
	UpdatePayload(streamName string, payload []byte) error

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
