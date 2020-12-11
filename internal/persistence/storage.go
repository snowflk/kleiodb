package persistence

// Storage is the interface to low-level storage module.
// The storage must be able to store events with global ordering and local ordering in each views
// EventKeeper contains all operations on events and views ( streams and custom views on higher level module)
// ViewKeeper contains all operations on view metadata
// SnapshotKeeper contains all operations on snapshots
// The implementation of this interface must use the package "testsuite" for unit-testing, so that we can make sure
// the implementation satisfies all requirements
type Storage interface {
	EventKeeper
	ViewKeeper
	SnapshotKeeper
	ForceFlush() error
}
