package meta

type RetentionPolicyEnforcer struct {
}

/*
// StartRetentionPolicyEnforcement launches retention policy enforcement.
func (s *Server) StartRetentionPolicyEnforcement(checkInterval time.Duration) error {
	if checkInterval == 0 {
		return fmt.Errorf("retention policy check interval must be non-zero")
	}
	rpDone := make(chan struct{}, 0)
	s.rpDone = rpDone
	go func() {
		for {
			select {
			case <-rpDone:
				return
			case <-time.After(checkInterval):
				s.EnforceRetentionPolicies()
			}
		}
	}()
	return nil
}

// EnforceRetentionPolicies ensures that data that is aging-out due to retention policies
// is removed from the server.
func (s *Server) EnforceRetentionPolicies() {
	log.Println("retention policy enforcement check commencing")

	type group struct {
		Database  string
		Retention string
		ID        uint64
	}

	var groups []group
	// Only keep the lock while walking the shard groups, so the lock is not held while
	// any deletions take place across the cluster.
	func() {
		s.mu.RLock()
		defer s.mu.RUnlock()

		// Check all shard groups.
		for _, db := range s.databases {
			for _, rp := range db.policies {
				for _, g := range rp.shardGroups {
					if rp.Duration != 0 && g.EndTime.Add(rp.Duration).Before(time.Now().UTC()) {
						log.Printf("shard group %d, retention policy %s, database %s due for deletion",
							g.ID, rp.Name, db.name)
						groups = append(groups, group{Database: db.name, Retention: rp.Name, ID: g.ID})
					}
				}
			}
		}
	}()

	for _, g := range groups {
		if err := s.DeleteShardGroup(g.Database, g.Retention, g.ID); err != nil {
			log.Printf("failed to request deletion of shard group %d: %s", g.ID, err.Error())
		}
	}
}
*/
