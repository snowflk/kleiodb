package hybridlog

import (
	"time"
)

type CompactionMode int
type SyncPolicy int

const (
	TimeBased CompactionMode = iota
	FragmentationBased
	NoSync SyncPolicy = iota
	AlwaysSync
	SyncEverySecond
)

//
type HybridLog interface {
	Write(p []byte) error
	ReadAt(b []byte, off int64) (int, error)
	Size() int64
	Close() error
}

type Config struct {
	// SimpleHybridLog configurations
	// Path simply locates where to store the data. The file extension needs to be provided.
	Path string
	// HighWaterMark defines the fullness of buffer (in percent) at which the remapping process will be started
	HighWaterMark int
	// The size of buffer (in bytes) for keeping the new written data in memory.
	// The larger the buffer size is, the more memory will be consumed.
	BufferSize  int
	OpenTimeout time.Duration
	// SyncPolicy denotes when to perform fdatasync
	SyncPolicy SyncPolicy

	// Compactor configurations
	AutoCompaction bool
	CompactionMode CompactionMode
	// CompactAfter defines when to launch a compaction process.
	// If CompactionMode is TimeBased, the value is the number of seconds.
	// If CompactionMode is FragmentationBased, the value is the number of fragments.
	CompactAfter int
	// The maximum size of the chunks to copy
	CompactionChunkSize int
	//CustomCompactor     func(src io.ReadSeeker, dst io.Writer)
}

// Open opens a hybrid log at the given path in config.
// If the log does not exist, it creates a new one.
//
func Open(cfg Config) (HybridLog, error) {
	log, err := open(cfg)
	if err != nil {
		return nil, err
	}
	// If Auto compaction mode is enabled,
	// wrap the stage by the compactor
	if cfg.AutoCompaction {
		return withCompactor(log, cfg)
	}

	return log, nil
}
