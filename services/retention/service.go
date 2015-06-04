package retention

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/influxdb/influxdb/meta"
	"github.com/influxdb/influxdb/tsdb"
)

// Service represents the retention policy enforcement service.
type Service struct {
	Enforcer interface {
		Enforce() ([]uint64, error)
	}
	TSDBStore interface {
		DeleteShard(shardID uint64) error
	}

	enabled       bool
	checkInterval time.Duration
	wg            sync.WaitGroup
	done          chan struct{}

	logger *log.Logger
}

// NewService returns a configure retention policy enforcement service.
func NewService(c *Config) *Service {
	return &Service{
		enabled:       c.Enabled,
		checkInterval: c.CheckInterval,
		logger:        log.New(os.Stderr, "[retention] ", log.LstdFlags),
	}
}

// Open starts retention policy enforcement.
func (s *Service) Open() error {
	if !s.enabled {
		return nil
	}

	s.wg.Add(1)
	go s.run()
	return nil
}

// Close stops retention policy enforcement.
func (s *Service) Close() error {
	close(s.done)
	s.wg.Wait()
	return nil
}

func (s *Service) run() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.checkInterval)
	for {
		select {
		case <-s.done:
			s.logger.Println("retention policy enforcement terminating")
			return

		case <-ticker.C:
			s.logger.Println("retention policy enforcement check commencing")
			ids, err := s.Enforcer.Enforce()
			if err != nil {
				log.Printf("error enforcing retention policies: %s", err.Error())
				continue
			}
			for id := range ids {
				if err := s.TSDBStore.DeleteShard(id); err != nil {
					log.Printf("failed to delete shard ID %d: %s", id, err.Error())
				} else {
					log.Printf("shard ID %d successfully deleted", id)
				}
			}
		}
	}
}
