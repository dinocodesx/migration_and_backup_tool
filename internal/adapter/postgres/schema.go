package postgres

import (
	"context"
	"fmt"

	"github.com/dinocodesx/gomigrate/internal/schema"
	"github.com/jackc/pgx/v5/pgxpool"
)

// GetSchema introspects the PostgreSQL database to get the canonical schema of
// a table. It queries the information_schema.columns table to retrieve column
// names, data types, nullability, and default values.
//
// The query filters by table_schema to avoid collisions when multiple
// schemas contain tables with the same name. It also identifies primary key
// columns by joining information_schema.table_constraints and
// information_schema.key_column_usage.
func GetSchema(ctx context.Context, db *pgxpool.Pool, table, tableSchema string) (*schema.Schema, error) {
	if tableSchema == "" {
		tableSchema = "public"
	}

	query := `
		SELECT
			column_name,
			data_type,
			is_nullable,
			column_default
		FROM information_schema.columns
		WHERE table_name = $1
		  AND table_schema = $2
		ORDER BY ordinal_position`

	rows, err := db.Query(ctx, query, table, tableSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to query information_schema for table %s.%s: %w", tableSchema, table, err)
	}
	defer rows.Close()

	s := &schema.Schema{Name: table}
	for rows.Next() {
		var colName, dataType, isNullable string
		var colDefault *string
		if err := rows.Scan(&colName, &dataType, &isNullable, &colDefault); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}

		s.Columns = append(s.Columns, schema.Column{
			Name:     colName,
			Type:     mapPostgresType(dataType),
			Nullable: isNullable == "YES",
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error during schema introspection: %w", err)
	}

	if len(s.Columns) == 0 {
		return nil, fmt.Errorf("table %q not found in schema %q", table, tableSchema)
	}

	// Identify primary key column(s).
	pkQuery := `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
		  AND tc.table_name   = $1
		  AND tc.table_schema = $2`

	pkRows, err := db.Query(ctx, pkQuery, table, tableSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary key for table %s: %w", table, err)
	}
	defer pkRows.Close()

	pkCols := make(map[string]bool)
	for pkRows.Next() {
		var pkCol string
		if err := pkRows.Scan(&pkCol); err != nil {
			return nil, fmt.Errorf("failed to scan pk column: %w", err)
		}
		pkCols[pkCol] = true
	}
	if err := pkRows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error during PK introspection: %w", err)
	}

	for i := range s.Columns {
		if pkCols[s.Columns[i].Name] {
			s.Columns[i].PrimaryKey = true
		}
	}

	return s, nil
}

// mapPostgresType converts a PostgreSQL data_type string (from information_schema)
// to the canonical gomigrate type string used for schema mapping and record validation.
func mapPostgresType(pgType string) string {
	switch pgType {
	case "integer", "bigint", "smallint", "int", "int2", "int4", "int8":
		return "int64"
	case "text", "character varying", "varchar", "char", "uuid", "name":
		return "string"
	case "boolean":
		return "bool"
	case "double precision", "numeric", "real", "float4", "float8":
		return "float64"
	case "timestamp with time zone", "timestamp without time zone",
		"timestamptz", "date":
		return "timestamp"
	case "jsonb", "json":
		return "map"
	case "bytea":
		return "blob"
	case "ARRAY":
		return "array"
	default:
		return "string" // safe fallback — stored as text
	}
}
