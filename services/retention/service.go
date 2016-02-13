package retention // import "github.com/influxdata/influxdb/services/retention"

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/influxdata/influxdb/services/meta"
)

// Service represents the retention policy enforcement service.
type Service struct {
	MetaClient interface {
		Databases() ([]meta.DatabaseInfo, error)
		DeleteShardGroup(database, policy string, id uint64) error
	}
	TSDBStore interface {
		ShardIDs() []uint64
		DeleteShard(shardID uint64) error
	}

	enabled       bool
	checkInterval time.Duration
	wg            sync.WaitGroup
	done          chan struct{}

	logger *log.Logger
}

// NewService returns a configured retention policy enforcement service.
func NewService(c Config) *Service {
	return &Service{
		checkInterval: time.Duration(c.CheckInterval),
		done:          make(chan struct{}),
		logger:        log.New(os.Stderr, "[retention] ", log.LstdFlags),
	}
}

// Open starts retention policy enforcement.
func (s *Service) Open() error {
	s.logger.Println("Starting retention policy enforcement service with check interval of", s.checkInterval)
	s.wg.Add(2)
	go s.deleteShardGroups()
	go s.deleteShards()
	return nil
}

// Close stops retention policy enforcement.
func (s *Service) Close() error {
	s.logger.Println("retention policy enforcement terminating")
	close(s.done)
	s.wg.Wait()
	return nil
}

// SetLogger sets the internal logger to the logger passed in.
func (s *Service) SetLogger(l *log.Logger) {
	s.logger = l
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
			dbs, err := s.MetaClient.Databases()
			if err != nil {
				s.logger.Printf("error getting databases: %s", err.Error())
				continue
			}

			for _, d := range dbs {
				for _, r := range d.RetentionPolicies {
					for _, g := range r.ExpiredShardGroups(time.Now().UTC()) {
						if err := s.MetaClient.DeleteShardGroup(d.Name, r.Name, g.ID); err != nil {
							s.logger.Printf("failed to delete shard group %d from database %s, retention policy %s: %s",
								g.ID, d.Name, r.Name, err.Error())
						} else {
							s.logger.Printf("deleted shard group %d from database %s, retention policy %s",
								g.ID, d.Name, r.Name)
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
			s.logger.Println("retention policy shard deletion check commencing")

			type deletionInfo struct {
				db string
				rp string
			}
			deletedShardIDs := make(map[uint64]deletionInfo, 0)
			dbs, err := s.MetaClient.Databases()
			if err != nil {
				s.logger.Printf("error getting databases: %s", err.Error())
			}
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
				if di, ok := deletedShardIDs[id]; ok {
					if err := s.TSDBStore.DeleteShard(id); err != nil {
						s.logger.Printf("failed to delete shard ID %d from database %s, retention policy %s: %s",
							id, di.db, di.rp, err.Error())
						continue
					}
					s.logger.Printf("shard ID %d from database %s, retention policy %s, deleted",
						id, di.db, di.rp)
				}
			}
		}
	}
}
