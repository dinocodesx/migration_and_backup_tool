package cassandra

import (
	"context"
	"fmt"
	"strings"

	"github.com/dinocodesx/gomigrate/internal/record"
	"github.com/dinocodesx/gomigrate/internal/schema"
	"github.com/gocql/gocql"
)

type Writer struct {
	session  *gocql.Session
	keyspace string
	table    string
}

func NewWriter(session *gocql.Session, keyspace, table string) *Writer {
	return &Writer{session: session, keyspace: keyspace, table: table}
}

func (w *Writer) WriteBatch(ctx context.Context, batch []*record.Record) (int, error) {
	if len(batch) == 0 {
		return 0, nil
	}

	// Group by 100 to avoid large batch warnings
	const maxBatchSize = 100
	totalWritten := 0

	for i := 0; i < len(batch); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(batch) {
			end = len(batch)
		}

		subBatch := batch[i:end]
		b := w.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)

		for _, rec := range subBatch {
			cols := make([]string, 0, len(rec.Data))
			placeholders := make([]string, 0, len(rec.Data))
			vals := make([]any, 0, len(rec.Data))

			for k, v := range rec.Data {
				cols = append(cols, k)
				placeholders = append(placeholders, "?")
				vals = append(vals, v)
			}

			stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", w.table, strings.Join(cols, ","), strings.Join(placeholders, ","))
			b.Query(stmt, vals...)
		}

		if err := w.session.ExecuteBatch(b); err != nil {
			return totalWritten, fmt.Errorf("failed to execute cassandra batch: %w", err)
		}
		totalWritten += len(subBatch)
	}

	return totalWritten, nil
}

func (w *Writer) ApplySchema(ctx context.Context, s *schema.Schema) error {
	w.table = s.Name

	var pkCols []string
	var colDefs []string

	for _, col := range s.Columns {
		cqlType := mapToCassandraType(col.Type)
		colDefs = append(colDefs, fmt.Sprintf("%s %s", col.Name, cqlType))
		if col.PrimaryKey {
			pkCols = append(pkCols, col.Name)
		}
	}

	if len(pkCols) == 0 {
		return fmt.Errorf("no primary key defined for table %s", s.Name)
	}

	// In Cassandra, composite PKs are (part1, part2, ...).
	// To simplify, we treat all PK columns as the partition key.
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY ((%s)))",
		s.Name,
		strings.Join(colDefs, ", "),
		strings.Join(pkCols, ", "))

	if err := w.session.Query(query).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("failed to apply schema to cassandra: %w", err)
	}

	return nil
}

func mapToCassandraType(t string) string {
	switch t {
	case "int64":
		return "bigint"
	case "string":
		return "text"
	case "bool":
		return "boolean"
	case "float64":
		return "double"
	case "timestamp":
		return "timestamp"
	case "array":
		return "list<text>" // Defaulting to list of text for generic arrays
	case "map":
		return "map<text, text>" // Defaulting to map of text to text
	case "blob":
		return "blob"
	default:
		return "text"
	}
}
