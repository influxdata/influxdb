// Package retention provides the retention policy enforcement service.
package retention // import "github.com/influxdata/influxdb/services/retention"

import (
	"sync"
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/retention/diagnostic"
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

	checkInterval time.Duration
	wg            sync.WaitGroup
	done          chan struct{}

	diag diagnostic.Context
}

// NewService returns a configured retention policy enforcement service.
func NewService(c Config) *Service {
	return &Service{
		checkInterval: time.Duration(c.CheckInterval),
		done:          make(chan struct{}),
	}
}

// Open starts retention policy enforcement.
func (s *Service) Open() error {
	if s.diag != nil {
		s.diag.Starting(s.checkInterval)
	}
	s.wg.Add(2)
	go s.deleteShardGroups()
	go s.deleteShards()
	return nil
}

// Close stops retention policy enforcement.
func (s *Service) Close() error {
	if s.diag != nil {
		s.diag.Closing()
	}
	close(s.done)
	s.wg.Wait()
	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) With(d diagnostic.Context) {
	s.diag = d
}

func (s *Service) deleteShardGroups() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return

		case <-ticker.C:
			dbs := s.MetaClient.Databases()
			for _, d := range dbs {
				for _, r := range d.RetentionPolicies {
					for _, g := range r.ExpiredShardGroups(time.Now().UTC()) {
						if err := s.MetaClient.DeleteShardGroup(d.Name, r.Name, g.ID); err != nil {
							if s.diag != nil {
								s.diag.DeleteShardGroupError(g.ID, d.Name, r.Name, err)
							}
						} else if s.diag != nil {
							s.diag.DeletedShardGroup(g.ID, d.Name, r.Name)
						}
					}
				}
			}
		}
	}
}

func (s *Service) deleteShards() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return

		case <-ticker.C:
			if s.diag != nil {
				s.diag.StartingCheck()
			}

			type deletionInfo struct {
				db string
				rp string
			}
			deletedShardIDs := make(map[uint64]deletionInfo, 0)
			dbs := s.MetaClient.Databases()
			for _, d := range dbs {
				for _, r := range d.RetentionPolicies {
					for _, g := range r.DeletedShardGroups() {
						for _, sh := range g.Shards {
							deletedShardIDs[sh.ID] = deletionInfo{db: d.Name, rp: r.Name}
						}
					}
				}
			}

			for _, id := range s.TSDBStore.ShardIDs() {
				if info, ok := deletedShardIDs[id]; ok {
					if err := s.TSDBStore.DeleteShard(id); err != nil {
						if s.diag != nil {
							s.diag.DeleteShardError(id, info.db, info.rp, err)
						}
						continue
					}
					if s.diag != nil {
						s.diag.DeletedShard(id, info.db, info.rp)
					}
				}
			}
			if err := s.MetaClient.PruneShardGroups(); err != nil && s.diag != nil {
				s.diag.PruneShardGroupsError(err)
			}
		}
	}
}
