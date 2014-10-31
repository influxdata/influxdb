package cluster

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/influxdb/influxdb/common"
)

type ShardSpace struct {
	// required, must be unique within the database
	Name string `json:"name"`
	// required, a database has many shard spaces and a shard space belongs to a database
	Database string `json:"database"`
	// this is optional, if they don't set it, we'll set to /.*/
	Regex         string `json:"regex"`
	compiledRegex *regexp.Regexp
	// this is optional, if they don't set it, it will default to the storage.dir in the config
	RetentionPolicy   string `json:"retentionPolicy"`
	ShardDuration     string `json:"shardDuration"`
	ReplicationFactor uint32 `json:"replicationFactor"`
	Split             uint32 `json:"split"`
	shards            []*ShardData
}

const (
	SEVEN_DAYS                        = time.Hour * 24 * 7
	DEFAULT_SPLIT                     = 1
	DEFAULT_REPLICATION_FACTOR        = 1
	DEFAULT_SHARD_DURATION            = SEVEN_DAYS
	DEFAULT_RETENTION_POLICY_DURATION = 0
)

const InfiniteRetention = time.Duration(0)

func NewShardSpace(database, name string) *ShardSpace {
	s := &ShardSpace{
		Database: database,
		Name:     name,
		shards:   make([]*ShardData, 0),
	}
	s.SetDefaults()
	return s
}

func (s *ShardSpace) Validate(clusterConfig *ClusterConfiguration, checkForDb bool) error {
	if clusterConfig.ShardSpaceExists(s) {
		return fmt.Errorf("Shard space %s exists for db %s", s.Name, s.Database)
	}
	if checkForDb {
		if !clusterConfig.DatabaseExists(s.Database) {
			return fmt.Errorf("Database '%s' doesn't exist.", s.Database)
		}
	}

	if s.Name == "" {
		return fmt.Errorf("Shard space must have a name")
	}
	if s.Regex == "" {
		return fmt.Errorf("A regex is required for a shard space")
	}
	r, err := s.compileRegex(s.Regex)
	if err != nil {
		return fmt.Errorf("Error parsing regex: %s", err)
	}
	s.compiledRegex = r
	if s.Split == 0 {
		s.Split = DEFAULT_SPLIT
	}
	if s.ReplicationFactor == 0 {
		s.ReplicationFactor = DEFAULT_REPLICATION_FACTOR
	}
	if s.ShardDuration != "" {
		if _, err := common.ParseTimeDuration(s.ShardDuration); err != nil {
			return err
		}
	}
	if s.RetentionPolicy != "" && s.RetentionPolicy != "inf" {
		if _, err := common.ParseTimeDuration(s.RetentionPolicy); err != nil {
			return err
		}
	}
	return nil
}

func (s *ShardSpace) compileRegex(reg string) (*regexp.Regexp, error) {
	if strings.HasPrefix(reg, "/") {
		if strings.HasSuffix(reg, "/i") {
			reg = fmt.Sprintf("(?i)%s", reg[1:len(reg)-2])
		} else {
			reg = reg[1 : len(reg)-1]
		}
	}
	return regexp.Compile(reg)
}

func (s *ShardSpace) SetDefaults() {
	r, _ := regexp.Compile(".*")
	s.compiledRegex = r
	s.Split = DEFAULT_SPLIT
	s.ReplicationFactor = DEFAULT_REPLICATION_FACTOR
	s.Regex = "/.*/"
	s.RetentionPolicy = "inf"
	s.ShardDuration = "7d"
}

func (s *ShardSpace) MatchesSeries(name string) bool {
	if s.compiledRegex == nil {
		s.compiledRegex, _ = s.compileRegex(s.Regex)
	}
	return s.compiledRegex.MatchString(name)
}

func (s *ShardSpace) SecondsOfDuration() float64 {
	return s.ParsedShardDuration().Seconds()
}

func (s *ShardSpace) ParsedRetentionPeriod() time.Duration {
	if s.RetentionPolicy == "" {
		return DEFAULT_RETENTION_POLICY_DURATION
	} else if s.RetentionPolicy == "inf" {
		return InfiniteRetention
	}
	d, _ := common.ParseTimeDuration(s.RetentionPolicy)
	return time.Duration(d)
}

func (s *ShardSpace) ParsedShardDuration() time.Duration {
	if s.ShardDuration != "" {
		d, _ := common.ParseTimeDuration(s.ShardDuration)
		return time.Duration(d)
	}
	return DEFAULT_SHARD_DURATION
}

func (s *ShardSpace) UpdateFromSpace(space *ShardSpace) error {
	r, err := s.compileRegex(space.Regex)
	if err != nil {
		return err
	}
	s.Regex = space.Regex
	s.compiledRegex = r
	s.RetentionPolicy = space.RetentionPolicy
	s.ShardDuration = space.ShardDuration
	s.ReplicationFactor = space.ReplicationFactor
	s.Split = space.Split
	return nil
}
