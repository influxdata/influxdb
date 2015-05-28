package meta

type ShardGroupPrecreator struct{}

/*
// StartShardGroupsPreCreate launches shard group pre-create to avoid write bottlenecks.
func (s *Server) StartShardGroupsPreCreate(checkInterval time.Duration) error {
	if checkInterval == 0 {
		return fmt.Errorf("shard group pre-create check interval must be non-zero")
	}
	sgpcDone := make(chan struct{}, 0)
	s.sgpcDone = sgpcDone
	go func() {
		for {
			select {
			case <-sgpcDone:
				return
			case <-time.After(checkInterval):
				s.ShardGroupPreCreate(checkInterval)
			}
		}
	}()
	return nil
}

// ShardGroupPreCreate ensures that future shard groups and shards are created and ready for writing
// is removed from the server.
func (s *Server) ShardGroupPreCreate(checkInterval time.Duration) {
	panic("not yet implemented")

	log.Println("shard group pre-create check commencing")

	// For safety, we double the check interval to ensure we have enough time to create all shard groups
	// before they are needed, but as close to needed as possible.
	// This is a complete punt on optimization
	cutoff := time.Now().Add(checkInterval * 2).UTC()

	type group struct {
		Database  string
		Retention string
		ID        uint64
		Time      time.Time
	}

	var groups []group
	// Only keep the lock while walking the shard groups, so the lock is not held while
	// any deletions take place across the cluster.
	func() {
		s.mu.RLock()
		defer s.mu.RUnlock()

		// Check all shard groups.
		// See if they have a "future" shard group ready to write to
		// If not, create the next shard group, as well as each shard for the shardGroup
		for _, db := range s.databases {
			for _, rp := range db.policies {
				for _, g := range rp.shardGroups {
					// Check to see if it is going to end before our interval
					if g.EndTime.Before(cutoff) {
						log.Printf("pre-creating shard group for %d, retention policy %s, database %s", g.ID, rp.Name, db.name)
						groups = append(groups, group{Database: db.name, Retention: rp.Name, ID: g.ID, Time: g.EndTime.Add(1 * time.Nanosecond)})
					}
				}
			}
		}
	}()

	for _, g := range groups {
		if err := s.CreateShardGroupIfNotExists(g.Database, g.Retention, g.Time); err != nil {
			log.Printf("failed to request pre-creation of shard group %d for time %s: %s", g.ID, g.Time, err.Error())
		}
	}
}
*/
