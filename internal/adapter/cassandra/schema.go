package cassandra

import (
	"context"
	"fmt"

	"github.com/dinocodesx/gomigrate/internal/schema"
	"github.com/gocql/gocql"
)

// GetSchema introspects the Cassandra table using Metadata API.
func GetSchema(ctx context.Context, session *gocql.Session, keyspace, table string) (*schema.Schema, error) {
	ksMetadata, err := session.KeyspaceMetadata(keyspace)
	if err != nil {
		return nil, fmt.Errorf("failed to get keyspace metadata: %w", err)
	}

	tableMetadata, ok := ksMetadata.Tables[table]
	if !ok {
		return nil, fmt.Errorf("table %s not found in keyspace %s", table, keyspace)
	}

	s := &schema.Schema{
		Name:    table,
		Columns: make([]schema.Column, 0, len(tableMetadata.Columns)),
	}

	pkMap := make(map[string]bool)
	for _, col := range tableMetadata.PartitionKey {
		pkMap[col.Name] = true
	}
	for _, col := range tableMetadata.ClusteringColumns {
		pkMap[col.Name] = true
	}

	for _, colName := range tableMetadata.OrderedColumns {
		colMetadata := tableMetadata.Columns[colName]
		s.Columns = append(s.Columns, schema.Column{
			Name:       colName,
			Type:       mapCassandraType(colMetadata.Type),
			PrimaryKey: pkMap[colName],
			Nullable:   true, // Cassandra columns are generally nullable except PKs
		})
	}

	return s, nil
}

func mapCassandraType(t gocql.TypeInfo) string {
	switch t.Type() {
	case gocql.TypeInt, gocql.TypeBigInt, gocql.TypeSmallInt, gocql.TypeTinyInt:
		return "int64"
	case gocql.TypeFloat, gocql.TypeDouble, gocql.TypeDecimal:
		return "float64"
	case gocql.TypeBoolean:
		return "bool"
	case gocql.TypeTimestamp:
		return "timestamp"
	case gocql.TypeUUID, gocql.TypeTimeUUID:
		return "string" // canonical UUID format as string
	case gocql.TypeBlob:
		return "blob"
	case gocql.TypeList, gocql.TypeSet:
		return "array"
	case gocql.TypeMap:
		return "map"
	case gocql.TypeVarchar, gocql.TypeText, gocql.TypeAscii:
		return "string"
	default:
		return "string"
	}
}
