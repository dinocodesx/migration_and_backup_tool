package postgres

import (
	"context"
	"fmt"

	"github.com/dinocodesx/migration_and_backup_tool/internal/schema"
	"github.com/jackc/pgx/v5/pgxpool"
)

// GetSchema introspects the PostgreSQL database to get the schema of a table.
func GetSchema(ctx context.Context, db *pgxpool.Pool, table string) (*schema.Schema, error) {
	query := `
		SELECT 
			column_name, 
			data_type, 
			is_nullable,
			column_default
		FROM information_schema.columns 
		WHERE table_name = $1 
		ORDER BY ordinal_position`

	rows, err := db.Query(ctx, query, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query information_schema: %w", err)
	}
	defer rows.Close()

	s := &schema.Schema{Name: table}
	for rows.Next() {
		var colName, dataType, isNullable string
		var colDefault *string
		if err := rows.Scan(&colName, &dataType, &isNullable, &colDefault); err != nil {
			return nil, err
		}

		s.Columns = append(s.Columns, schema.Column{
			Name:     colName,
			Type:     mapPostgresType(dataType),
			Nullable: isNullable == "YES",
		})
	}

	// Identify primary key
	pkQuery := `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		  AND tc.table_schema = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = $1`

	var pkName string
	err = db.QueryRow(ctx, pkQuery, table).Scan(&pkName)
	if err == nil {
		for i := range s.Columns {
			if s.Columns[i].Name == pkName {
				s.Columns[i].PrimaryKey = true
				break
			}
		}
	}

	return s, nil
}

func mapPostgresType(pgType string) string {
	switch pgType {
	case "integer", "bigint", "smallint":
		return "int64"
	case "text", "character varying", "varchar", "uuid":
		return "string"
	case "boolean":
		return "bool"
	case "double precision", "numeric", "real":
		return "float64"
	case "timestamp with time zone", "timestamp without time zone", "date":
		return "timestamp"
	default:
		return "string" // Fallback
	}
}
