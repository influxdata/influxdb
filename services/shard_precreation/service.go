package shard_precreation

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/influxdb/influxdb/meta"
)

type Service struct {
	checkInterval time.Duration
	advancePeriod time.Duration

	Logger *log.Logger

	done chan struct{}
	wg   sync.WaitGroup

	MetaStore interface {
		IsLeader() bool
		VisitRetentionPolicies(f func(d meta.DatabaseInfo, r meta.RetentionPolicyInfo))
		ShardGroups(database, policy string) ([]meta.ShardGroupInfo, error)
		CreateShardGroupIfNotExists(database, policy string, timestamp time.Time) (*meta.ShardGroupInfo, error)
	}
}

// NewService returns an instance of the Graphite service.
func NewService(c Config) (*Service, error) {
	s := Service{
		checkInterval: time.Duration(c.CheckInterval),
		advancePeriod: time.Duration(c.AdvancePeriod),
		Logger:        log.New(os.Stderr, "[shard-precreation] ", log.LstdFlags),
	}

	return &s, nil
}

// SetLogger sets the internal logger to the logger passed in.
func (s *Service) SetLogger(l *log.Logger) {
	s.Logger = l
}

// Open starts the shard precreation service.
func (s *Service) Open() error {
	if s.done != nil {
		return nil
	}

	s.done = make(chan struct{})

	s.wg.Add(1)
	go s.runPrecreation()
	return nil
}

// Close stops the shard precreation service.
func (s *Service) Close() error {
	if s.done == nil {
		return nil
	}

	close(s.done)
	s.wg.Wait()
	s.done = nil

	return nil
}

// runPrecreation continually checks if shards need precreation.
func (s *Service) runPrecreation() {
	defer s.wg.Done()

	for {
		select {
		case <-time.After(s.checkInterval):
			// Only run this on the leader, but always allow the loop to check
			// as the leader can change.
			if !s.MetaStore.IsLeader() {
				continue
			}

			if _, err := s.precreate(time.Now().UTC()); err != nil {
				s.Logger.Printf("failed to precreate shards: %s", err.Error())
			}
		case <-s.done:
			s.Logger.Println("shard precreation service terminating")
			return
		}
	}
}

// precreate performs actual shard precreation. Returns the number of groups that were created.
func (s *Service) precreate(t time.Time) (int, error) {
	cutoff := t.Add(s.advancePeriod).UTC()
	numCreated := 0

	s.MetaStore.VisitRetentionPolicies(func(d meta.DatabaseInfo, r meta.RetentionPolicyInfo) {
		groups, err := s.MetaStore.ShardGroups(d.Name, r.Name)
		if err != nil {
			s.Logger.Printf("failed to retrieve shard groups for database %s, policy %s: %s",
				d.Name, r.Name, err.Error())
			return
		}
		for _, g := range groups {
			// Check to see if it is going to end before our interval
			if g.EndTime.Before(cutoff) {
				s.Logger.Printf("pre-creating successive shard group for group %d, database %s, policy %s",
					g.ID, d.Name, r.Name)
				if newGroup, err := s.MetaStore.CreateShardGroupIfNotExists(d.Name, r.Name, g.EndTime.Add(1*time.Nanosecond)); err != nil {
					s.Logger.Printf("failed to create successive shard group for group %d: %s",
						g.ID, err.Error())
				} else {
					numCreated++
					s.Logger.Printf("new shard group %d successfully created", newGroup.ID)
				}
			}
		}
	})
	return numCreated, nil
}
