package sqlimpl

// Settings configures where the store keeps its data: Path is the SQLite
// database file, created with its directory when it does not exist.
//
// Queues names queues the app creates at startup if they do not exist yet,
// with the default attributes; a name ending in .fifo is a FIFO queue's.
// The app reads it, and the store never does.
type Settings struct {
	Path   string `validate:"required"`
	Queues []string
}
