package sqlimpl

// Settings configures where the store keeps its data: Path is the SQLite
// database file, created with its directory when it does not exist.
type Settings struct {
	Path string `validate:"required"`
}
