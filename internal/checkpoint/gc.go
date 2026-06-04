package checkpoint

import (
	"encoding/json"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// GC (Garbage Collection) removes checkpoint data for jobs that were started
// longer ago than the specified 'retentionAge'. This prevents the Bolt database
// from growing indefinitely.
func GC(store *Store, logger *zap.Logger, retentionAge time.Duration) error {
	ids, err := store.ListAllMetaIDs()
	if err != nil {
		return fmt.Errorf("gc: failed to list operation IDs: %w", err)
	}

	cutoff := time.Now().Add(-retentionAge)
	for _, id := range ids {
		meta, err := store.GetMeta(id)
		if err != nil {
			logger.Warn("gc: skipping unreadable meta entry",
				zap.String("operation_id", id),
				zap.Error(err),
			)
			continue
		}

		if meta.StartTime.Before(cutoff) {
			logger.Info("gc: removing stale checkpoint",
				zap.String("operation_id", meta.OperationID),
				zap.Time("start_time", meta.StartTime),
			)
			if err := store.DeleteOperation(meta.OperationID); err != nil {
				logger.Warn("gc: failed to delete operation checkpoint",
					zap.String("operation_id", meta.OperationID),
					zap.Error(err),
				)
			}
		}
	}
	return nil
}

// ListAllMetaIDs returns a slice of all operation identifiers currently tracked
// in the metadata bucket.
func (s *Store) ListAllMetaIDs() ([]string, error) {
	var ids []string
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		return b.ForEach(func(k, _ []byte) error {
			id := string(k)
			ids = append(ids, id)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// ListAllMeta returns a slice of all global metadata objects in the store.
func (s *Store) ListAllMeta() ([]OperationMeta, error) {
	var metas []OperationMeta
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		return b.ForEach(func(_, v []byte) error {
			var meta OperationMeta
			if err := json.Unmarshal(v, &meta); err != nil {
				return err
			}
			metas = append(metas, meta)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return metas, nil
}
