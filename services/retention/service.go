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

		SetShardNewReadersBlocked(shardID uint64, blocked bool) error
		ShardInUse(shardID uint64) (bool, error)
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
				func() {
					log, logEnd := logger.NewOperation(log, "Deleting expired shard group", "retention_delete_expired_shard_group",
						logger.Database(d.Name), logger.ShardGroup(g.ID), logger.RetentionPolicy(r.Name))
					defer logEnd()
					if err := s.MetaClient.DeleteShardGroup(d.Name, r.Name, g.ID); err != nil {
						log.Error("Failed to delete shard group", zap.Error(err))
						retryNeeded = true
						return
					}

					log.Info("Deleted shard group")

					// Store all the shard IDs that may possibly need to be removed locally.
					groupShards := make([]uint64, len(g.Shards))
					for _, sh := range g.Shards {
						groupShards = append(groupShards, sh.ID)
						deletedShardIDs[sh.ID] = newDeletionInfo(d.Name, r.Name, sh)
					}
					log.Info("Group's shards will be removed from local storage if found", zap.Uint64s("shards", groupShards))
				}()
			}
		}
	}

	// Remove shards if we store them locally
	for _, id := range s.TSDBStore.ShardIDs() {
		if info, ok := deletedShardIDs[id]; ok {
			delete(deletedShardIDs, id)

			err := func() (rErr error) {
				log, logEnd := logger.NewOperation(log, "Deleting shard from shard group deleted based on retention policy", "retention_delete_shard",
					logger.Database(info.db), logger.Shard(id), logger.RetentionPolicy(info.rp))
				defer func() {
					if rErr != nil {
						// Log the error before logEnd().
						log.Error("Error deleting shard", zap.Error(rErr))
					}
					logEnd()
				}()

				// Block new readers for shard and check if it is in-use before deleting. This is to prevent
				// an issue where a shard that is stuck in-use will block the retention service.
				if err := s.TSDBStore.SetShardNewReadersBlocked(id, true); err != nil {
					return fmt.Errorf("error blocking new readers for shard: %w", err)
				}
				defer func() {
					if rErr != nil && !errors.Is(rErr, tsdb.ErrShardNotFound) {
						log.Info("Unblocking new readers for shard after delete failed")
						if unblockErr := s.TSDBStore.SetShardNewReadersBlocked(id, false); unblockErr != nil {
							log.Error("Error unblocking new readers for shard", zap.Error(unblockErr))
						}
					}
				}()

				// We should only try to delete shards that are not in-use.
				if inUse, err := s.TSDBStore.ShardInUse(id); err != nil {
					return fmt.Errorf("error checking if shard is in-use: %w", err)
				} else if inUse {
					return errors.New("can not delete an in-use shard")
				}

				// Now it's time to delete the shard
				if err := s.TSDBStore.DeleteShard(id); err != nil {
					return fmt.Errorf("error deleting shard from store: %w", err)
				}
				log.Info("Deleted shard")
				return nil
			}()
			// Check for error deleting the shard from the TSDB. Continue onto DropShardMetaRef if the
			// error was tsdb.ErrShardNotFound. We got here because the shard was in the metadata,
			// but it wasn't really in the store, so try deleting it out of the metadata.
			if err != nil && !errors.Is(err, tsdb.ErrShardNotFound) {
				// Logging of error was handled by the lambda in a defer so that it is within
				// the operation instead of after the operation.
				retryNeeded = true
				continue
			}

			func() {
				log, logEnd := logger.NewOperation(log, "Dropping shard meta references", "retention_drop_refs",
					logger.Database(info.db), logger.Shard(id), logger.RetentionPolicy(info.rp), zap.Uint64s("owners", info.owners))
				defer logEnd()
				if err := s.DropShardMetaRef(id, info.owners); err != nil {
					log.Error("Error dropping shard meta reference", zap.Error(err))
					retryNeeded = true
					return
				}
			}()
		}
	}

	// Check for expired phantom shards that exist in the metadata but not in the store.
	for id, info := range deletedShardIDs {
		func() {
			log, logEnd := logger.NewOperation(log, "Drop phantom shard references", "retention_drop_phantom_refs",
				logger.Database(info.db), logger.Shard(id), logger.RetentionPolicy(info.rp), zap.Uint64s("owners", info.owners))
			defer logEnd()
			log.Warn("Expired phantom shard detected during retention check, removing from metadata")
			if err := s.DropShardMetaRef(id, info.owners); err != nil {
				log.Error("Error dropping shard meta reference for phantom shard", zap.Error(err))
				retryNeeded = true
			}
		}()
	}

	func() {
		log, logEnd := logger.NewOperation(log, "Pruning shard groups after retention check", "retention_prune_shard_groups")
		defer logEnd()
		if err := s.MetaClient.PruneShardGroups(); err != nil {
			log.Error("Error pruning shard groups", zap.Error(err))
			retryNeeded = true
		}
	}()

	if retryNeeded {
		log.Info("One or more errors occurred during shard deletion and will be retried on the next check", logger.DurationLiteral("check_interval", time.Duration(s.config.CheckInterval)))
	}
}
