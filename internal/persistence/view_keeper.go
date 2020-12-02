package persistence

type ViewKeeper interface {
	// CreateViewMeta creates a view.
	// The view name should contain only digits, letters, underscore and dash symbol
	// Returns error if the view name already existed
	CreateViewMeta(viewName string, payload []byte) error

	// GetViewMeta returns a view and its metadata back
	GetViewMeta(viewName string) (RawView, error)

	// FindViews finds views using a pattern and returns their name back
	// If the pattern is invalid, the function just ignores it and returns an empty array
	FindViews(pattern Pattern) ([]string, error)

	// GetViewVersion returns the latest version of the given view
	// Returns an error if view does not exist
	GetViewVersion(viewName string) (uint32, error)

	// IncrementViewVersion increments the latest version of the view by a given amount
	IncrementViewVersion(viewName string, increment uint32) (uint32, error)

	// UpdateViewMeta updates payload of an existed view
	// Return error if the view name does not exist
	UpdateViewMeta(viewName string, payload []byte) error

	Close() error
}

type RawView struct {
	ViewName string
	Version  uint32
	Payload  []byte
}

type Pattern string

func (p Pattern) Match(name string) bool {
	panic("implement me")
}

func (p Pattern) String() string {
	return string(p)
}
