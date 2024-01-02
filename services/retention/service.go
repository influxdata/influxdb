// Package retention provides the retention policy enforcement service.
package retention // import "github.com/influxdata/influxdb/services/retention"

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

type MetaClient interface {
	Databases() []meta.DatabaseInfo
	DeleteShardGroup(database, policy string, id uint64) error
	DropShard(id uint64) error
	PruneShardGroups() error
}

// Service represents the retention policy enforcement service.
type Service struct {
	MetaClient
	TSDBStore interface {
		ShardIDs() []uint64
		DeleteShard(shardID uint64) error
	}

	// DropShardRef is a function that takes a shard ID and removes the
	// "reference" to it in the meta data. For OSS, this would be a DropShard
	// operation. For Enterprise, this would be a RemoveShardOwner operation.
	// Also provided is owners, the list of node IDs of the shard owners
	// according to the meta store. For OSS, owners will always be empty.
	// Enterprise can use owners to optimize out calls to RemoveShardOwner
	// if the current node doesn't actually own the shardID. This prevents
	// a lot of unnecessary RPC calls.
	DropShardMetaRef func(shardID uint64, owners []uint64) error

	config Config

	wg   sync.WaitGroup
	done chan struct{}

	logger *zap.Logger
}

// NewService returns a configured retention policy enforcement service.
func NewService(c Config) *Service {
	return &Service{
		config: c,
		logger: zap.NewNop(),
	}
}

// OSSDropShardMetaRef creates a closure appropriate for OSS to use as DropShardMetaRef.
func OSSDropShardMetaRef(mc MetaClient) func(uint64, []uint64) error {
	return func(shardID uint64, owners []uint64) error {
		return mc.DropShard(shardID)
	}
}

// Open starts retention policy enforcement.
func (s *Service) Open() error {
	if !s.config.Enabled || s.done != nil {
		return nil
	}

	if s.DropShardMetaRef == nil {
		return fmt.Errorf("invalid nil for retention service DropShardMetaRef")
	}

	s.logger.Info("Starting retention policy enforcement service",
		logger.DurationLiteral("check_interval", time.Duration(s.config.CheckInterval)))
	s.done = make(chan struct{})

	s.wg.Add(1)
	go func() { defer s.wg.Done(); s.run() }()
	return nil
}

// Close stops retention policy enforcement.
func (s *Service) Close() error {
	if !s.config.Enabled || s.done == nil {
		return nil
	}

	s.logger.Info("Closing retention policy enforcement service")
	close(s.done)

	s.wg.Wait()
	s.done = nil
	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.logger = log.With(zap.String("service", "retention"))
}

func (s *Service) run() {
	ticker := time.NewTicker(time.Duration(s.config.CheckInterval))
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return

		case <-ticker.C:
			s.DeletionCheck()
		}
	}
}

func (s *Service) DeletionCheck() {
	log, logEnd := logger.NewOperation(s.logger, "Retention policy deletion check", "retention_delete_check")
	defer logEnd()

	type deletionInfo struct {
		db     string
		rp     string
		owners []uint64
	}
	newDeletionInfo := func(db, rp string, si meta.ShardInfo) deletionInfo {
		owners := make([]uint64, len(si.Owners))
		for i, o := range si.Owners {
			owners[i] = o.NodeID
		}
		return deletionInfo{db: db, rp: rp, owners: owners}
	}
	deletedShardIDs := make(map[uint64]deletionInfo)

	dropShardMetaRef := func(id uint64, info deletionInfo) error {
		if err := s.DropShardMetaRef(id, info.owners); err != nil {
			log.Error("Failed to drop shard meta reference",
				logger.Database(info.db),
				logger.Shard(id),
				logger.RetentionPolicy(info.rp),
				zap.Error(err))
			return err
		}
		return nil
	}

	// Mark down if an error occurred during this function so we can inform the
	// user that we will try again on the next interval.
	// Without the message, they may see the error message and assume they
	// have to do it manually.
	var retryNeeded bool
	dbs := s.MetaClient.Databases()
	for _, d := range dbs {
		for _, r := range d.RetentionPolicies {
			// Build list of already deleted shards.
			for _, g := range r.DeletedShardGroups() {
				for _, sh := range g.Shards {
					deletedShardIDs[sh.ID] = newDeletionInfo(d.Name, r.Name, sh)
				}
			}

			// Determine all shards that have expired and need to be deleted.
			for _, g := range r.ExpiredShardGroups(time.Now().UTC()) {
				if err := s.MetaClient.DeleteShardGroup(d.Name, r.Name, g.ID); err != nil {
					log.Info("Failed to delete shard group",
						logger.Database(d.Name),
						logger.ShardGroup(g.ID),
						logger.RetentionPolicy(r.Name),
						zap.Error(err))
					retryNeeded = true
					continue
				}

				log.Info("Deleted shard group",
					logger.Database(d.Name),
					logger.ShardGroup(g.ID),
					logger.RetentionPolicy(r.Name))

				// Store all the shard IDs that may possibly need to be removed locally.
				for _, sh := range g.Shards {
					deletedShardIDs[sh.ID] = newDeletionInfo(d.Name, r.Name, sh)
				}
			}
		}
	}

	// Remove shards if we store them locally
	for _, id := range s.TSDBStore.ShardIDs() {
		if info, ok := deletedShardIDs[id]; ok {
			delete(deletedShardIDs, id)
			if err := s.TSDBStore.DeleteShard(id); err != nil {
				log.Error("Failed to delete shard",
					logger.Database(info.db),
					logger.Shard(id),
					logger.RetentionPolicy(info.rp),
					zap.Error(err))
				if errors.Is(err, tsdb.ErrShardNotFound) {
					// At first you wouldn't think this could happen, we're iterating over shards
					// in the store. However, if this has been a very long running operation the
					// shard could have been dropped from the store while we were working on other shards.
					log.Warn("Shard does not exist in store, continuing retention removal",
						logger.Database(info.db),
						logger.Shard(id),
						logger.RetentionPolicy(info.rp))
				} else {
					retryNeeded = true
					continue
				}
			}
			log.Info("Deleted shard",
				logger.Database(info.db),
				logger.Shard(id),
				logger.RetentionPolicy(info.rp))
			if err := dropShardMetaRef(id, info); err != nil {
				// removeShardMetaReference already logged the error.
				retryNeeded = true
				continue
			}
		}
	}

	// Check for expired phantom shards that exist in the metadata but not in the store.
	for id, info := range deletedShardIDs {
		log.Error("Expired phantom shard detected during retention check, removing from metadata",
			logger.Database(info.db),
			logger.Shard(id),
			logger.RetentionPolicy(info.rp))
		if err := dropShardMetaRef(id, info); err != nil {
			// removeShardMetaReference already logged the error.
			retryNeeded = true
			continue
		}
	}

	if err := s.MetaClient.PruneShardGroups(); err != nil {
		log.Info("Problem pruning shard groups", zap.Error(err))
		retryNeeded = true
	}

	if retryNeeded {
		log.Info("One or more errors occurred during shard deletion and will be retried on the next check", logger.DurationLiteral("check_interval", time.Duration(s.config.CheckInterval)))
	}
}
