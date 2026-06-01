package checkpoint

import (
	"encoding/json"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// GC removes checkpoint data for operations whose start time is older than
// retentionAge. It is safe to call periodically or at startup.
//
// Typical usage: call GC at startup with a retention of 7 days so the bolt
// file doesn't grow unboundedly over many migration runs.
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

// ListAllMetaIDs returns the operation IDs of every entry in the meta bucket.
// It is a low-level helper exposed for GC and tooling.
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

// ListAllMeta returns every OperationMeta stored in the meta bucket.
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
