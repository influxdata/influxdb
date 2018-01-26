// Package retention provides the retention policy enforcement service.
package retention // import "github.com/influxdata/influxdb/services/retention"

import (
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

// Service represents the retention policy enforcement service.
type Service struct {
	MetaClient interface {
		Databases() []meta.DatabaseInfo
		DeleteShardGroup(database, policy string, id uint64) error
		PruneShardGroups() error
	}
	TSDBStore interface {
		ShardIDs() []uint64
		DeleteShard(shardID uint64) error
	}

	config Config
	wg     sync.WaitGroup
	done   chan struct{}

	logger *zap.Logger
}

// NewService returns a configured retention policy enforcement service.
func NewService(c Config) *Service {
	return &Service{
		config: c,
		logger: zap.NewNop(),
	}
}

// Open starts retention policy enforcement.
func (s *Service) Open() error {
	if !s.config.Enabled || s.done != nil {
		return nil
	}

	s.logger.Info("Starting retention policy enforcement service", zap.String("check-interval", s.config.CheckInterval.String()))
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

	s.logger.Info("Retention policy enforcement service closing.")
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
			s.logger.Info("Retention policy shard deletion check commencing.")

			type deletionInfo struct {
				db string
				rp string
			}
			deletedShardIDs := make(map[uint64]deletionInfo)

			dbs := s.MetaClient.Databases()
			for _, d := range dbs {
				for _, r := range d.RetentionPolicies {
					for _, g := range r.ExpiredShardGroups(time.Now().UTC()) {
						if err := s.MetaClient.DeleteShardGroup(d.Name, r.Name, g.ID); err != nil {
							s.logger.Info(fmt.Sprintf("Failed to delete shard group %d from database %s, retention policy %s: %v. Retry in %v.", g.ID, d.Name, r.Name, err, s.config.CheckInterval))
							continue
						}

						s.logger.Info(fmt.Sprintf("Deleted shard group %d from database %s, retention policy %s.", g.ID, d.Name, r.Name))

						// Store all the shard IDs that may possibly need to be removed locally.
						for _, sh := range g.Shards {
							deletedShardIDs[sh.ID] = deletionInfo{db: d.Name, rp: r.Name}
						}
					}
				}
			}

			// Remove shards if we store them locally
			for _, id := range s.TSDBStore.ShardIDs() {
				if info, ok := deletedShardIDs[id]; ok {
					if err := s.TSDBStore.DeleteShard(id); err != nil {
						s.logger.Error(fmt.Sprintf("Failed to delete shard ID %d from database %s, retention policy %s: %v. Will retry in %v", id, info.db, info.rp, err, s.config.CheckInterval))
						continue
					}
					s.logger.Info(fmt.Sprintf("Shard ID %d from database %s, retention policy %s, deleted.", id, info.db, info.rp))
				}
			}

			if err := s.MetaClient.PruneShardGroups(); err != nil {
				s.logger.Info(fmt.Sprintf("Problem pruning shard groups: %s. Will retry in %v", err, s.config.CheckInterval))
			}
		}
	}
}
