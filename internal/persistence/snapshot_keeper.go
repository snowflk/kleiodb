package persistence

type SnapshotKeeper interface {
	SaveSnapshot(key string, data []byte) error

	GetSnapshot(key string) ([]byte, error)

	Close() error
}
