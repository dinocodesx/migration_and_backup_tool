// Package checkpoint implements persistent progress tracking for gomigrate.
// It uses bbolt (an embedded key-value store) to ensure that migration state
// survives application crashes and allows for efficient job resumption.
package checkpoint

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

var (
	// bucketMeta stores global OperationMeta objects.
	bucketMeta = []byte("meta")
	// bucketPartitions stores PartitionCheckpoint objects, keyed by opID:partitionID.
	bucketPartitions = []byte("partitions")

	// ErrCheckpointNotFound is returned when a requested record is missing from the store.
	ErrCheckpointNotFound = fmt.Errorf("checkpoint not found")
)

// Store provides a high-level API for managing checkpoint data. It handles
// bucket initialization and provides thread-safe access to bbolt.
type Store struct {
	// db is the underlying bbolt database instance.
	db *bbolt.DB
}

// NewStore opens a bbolt database at the specified path and initializes
// the required buckets. It uses a 1-second timeout to prevent blocking on stale locks.
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
		_ = db.Close()
		return nil, fmt.Errorf("failed to initialize buckets: %w", err)
	}

	return &Store{db: db}, nil
}

// Close releases the lock on the bbolt database file.
func (s *Store) Close() error {
	return s.db.Close()
}

// SaveMeta persists global operation metadata.
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

// GetMeta retrieves global metadata for a specific operation ID.
func (s *Store) GetMeta(opID string) (*OperationMeta, error) {
	var meta OperationMeta
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		data := b.Get([]byte(opID))
		if data == nil {
			return ErrCheckpointNotFound
		}
		return json.Unmarshal(data, &meta)
	})
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

// SavePartition persists the progress of a specific partition.
func (s *Store) SavePartition(opID string, cp PartitionCheckpoint) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketPartitions)
		data, err := json.Marshal(cp)
		if err != nil {
			return err
		}
		return b.Put(partitionKey(opID, cp.PartitionID), data)
	})
}

// GetPartition retrieves the current progress of a specific partition.
func (s *Store) GetPartition(opID, partitionID string) (*PartitionCheckpoint, error) {
	var cp PartitionCheckpoint
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketPartitions)
		data := b.Get(partitionKey(opID, partitionID))
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

// ListPartitions returns all progress records associated with a specific operation ID.
func (s *Store) ListPartitions(opID string) ([]PartitionCheckpoint, error) {
	prefix := partitionKeyPrefix(opID)
	var checkpoints []PartitionCheckpoint

	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketPartitions)
		c := b.Cursor()

		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			dec := json.NewDecoder(bytes.NewReader(v))
			dec.UseNumber()
			var cp PartitionCheckpoint
			if err := dec.Decode(&cp); err != nil {
				return fmt.Errorf("failed to decode partition %s: %w", string(k), err)
			}
			checkpoints = append(checkpoints, cp)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return checkpoints, nil
}

// SaveStatus updates only the operational state of a partition.
func (s *Store) SaveStatus(opID, partitionID string, status PartitionStatus) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketPartitions)
		key := partitionKey(opID, partitionID)
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

// DeleteOperation wipes all data (metadata and partition progress) for an operation.
func (s *Store) DeleteOperation(opID string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.Bucket(bucketMeta).Delete([]byte(opID)); err != nil {
			return err
		}

		prefix := partitionKeyPrefix(opID)
		b := tx.Bucket(bucketPartitions)
		c := b.Cursor()
		var keysToDelete [][]byte
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			kCopy := make([]byte, len(k))
			copy(kCopy, k)
			keysToDelete = append(keysToDelete, kCopy)
		}
		for _, k := range keysToDelete {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func partitionKey(opID, partitionID string) []byte {
	return []byte(opID + ":" + partitionID)
}

func partitionKeyPrefix(opID string) []byte {
	return []byte(opID + ":")
}
