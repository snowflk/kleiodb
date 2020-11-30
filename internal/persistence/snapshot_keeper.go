package persistence

type SnapshotKeeper interface {
	Save(key string, data []byte) error

	Get(key string) ([]byte, error)

	Close() error
}
