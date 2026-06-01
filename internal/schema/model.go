package schema

// Schema represents the structure of a database table or collection.
type Schema struct {
	Name    string
	Columns []Column
}

// Column represents a single field in a schema.
type Column struct {
	Name       string
	Type       string
	Nullable   bool
	PrimaryKey bool
}
