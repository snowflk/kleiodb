package persistence

import "github.com/pkg/errors"

//TODO: rename errors
var (
	ErrViewEmpty            = errors.New("view name cannot empty")
	ErrStreamNotExist       = errors.New("stream does not exist")
	ErrStreamEmpty          = errors.New("stream name cannot empty")
	ErrStreamInvalid        = errors.New("stream name should contain only digits, letters, underscore and dash symbol")
	ErrDataInvalid          = errors.New("data is invalid")
	ErrDataEmpty            = errors.New("data cannot empty")
	ErrSnapshotNameEmpty    = errors.New("snapshot's name cannot empty")
	ErrSnapshotSourceEmpty  = errors.New("snapshot's source cannot empty")
	ErrSnapshotPatternEmpty = errors.New("pattern cannot empty")
)
