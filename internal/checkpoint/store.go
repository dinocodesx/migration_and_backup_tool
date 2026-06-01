package checkpoint

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

var (
	bucketMeta       = []byte("meta")
	bucketPartitions = []byte("partitions")

	ErrCheckpointNotFound = fmt.Errorf("checkpoint not found")
)

// Store is a bbolt-backed checkpoint store.
type Store struct {
	db *bbolt.DB
}

// NewStore opens a bbolt database at the given path.
func NewStore(path string) (*Store, error) {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("failed to open bbolt db: %w", err)
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketMeta); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(bucketPartitions); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			return nil, fmt.Errorf("failed to initialize buckets: %v; close error: %w", err, closeErr)
		}
		return nil, fmt.Errorf("failed to initialize buckets: %w", err)
	}

	return &Store{db: db}, nil
}

// Close closes the underlying database.
func (s *Store) Close() error {
	return s.db.Close()
}

// SaveMeta saves the operation metadata.
func (s *Store) SaveMeta(meta OperationMeta) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		data, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		return b.Put([]byte(meta.OperationID), data)
	})
}

// SavePartition saves a partition checkpoint.
func (s *Store) SavePartition(opID string, cp PartitionCheckpoint) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketPartitions)
		data, err := json.Marshal(cp)
		if err != nil {
			return err
		}
		return b.Put([]byte(fmt.Sprintf("%s:%s", opID, cp.PartitionID)), data)
	})
}

// GetPartition retrieves a partition checkpoint.
func (s *Store) GetPartition(opID, partitionID string) (*PartitionCheckpoint, error) {
	var cp PartitionCheckpoint
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketPartitions)
		data := b.Get([]byte(fmt.Sprintf("%s:%s", opID, partitionID)))
		if data == nil {
			return ErrCheckpointNotFound
		}

		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		return dec.Decode(&cp)
	})
	if err != nil {
		return nil, err
	}
	return &cp, nil
}

// SaveStatus updates only the status of a partition.
func (s *Store) SaveStatus(opID, partitionID string, status PartitionStatus) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketPartitions)
		key := []byte(fmt.Sprintf("%s:%s", opID, partitionID))
		data := b.Get(key)
		if data == nil {
			return ErrCheckpointNotFound
		}

		var cp PartitionCheckpoint
		if err := json.Unmarshal(data, &cp); err != nil {
			return err
		}

		cp.Status = status
		cp.UpdatedAt = time.Now()
		
		newData, err := json.Marshal(cp)
		if err != nil {
			return err
		}
		return b.Put(key, newData)
	})
}
