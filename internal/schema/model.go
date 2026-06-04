// Package schema defines the structures used to represent database schemas
// in a database-agnostic format. It facilitates type mapping and structural
// validation during migration and backup operations.
package schema

// Schema represents the structural definition of a database table or collection.
// It serves as a blueprint for creating target tables and validating data records.
type Schema struct {
	// Name is the identifier of the table or collection.
	Name string
	// Columns is an ordered slice of field definitions.
	Columns []Column
}

// Column represents a single attribute or field within a schema.
type Column struct {
	// Name is the identifier of the column.
	Name string
	// Type is the canonical data type string (e.g., "int64", "string", "timestamp").
	Type string
	// Nullable indicates if the column can store null values.
	Nullable bool
	// PrimaryKey indicates if this column is part of the table's unique identifier.
	PrimaryKey bool
	// AllowedValues is an optional list of valid values (useful for ENUM types).
	AllowedValues []string
}
