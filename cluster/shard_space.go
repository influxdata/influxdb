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

func NewShardSpace(database, name string) *ShardSpace {
	s := &ShardSpace{
		Database: database,
		Name:     name,
		shards:   make([]*ShardData, 0),
	}
	s.SetDefaults()
	return s
}

func (s *ShardSpace) Validate(clusterConfig *ClusterConfiguration) error {
	if err := clusterConfig.DoesShardSpaceExist(s); err != nil {
		return err
	}

	if s.Name == "" {
		return fmt.Errorf("Shard space must have a name")
	}
	if s.Regex != "" {
		reg := s.Regex
		if strings.HasPrefix(reg, "/") {
			if strings.HasSuffix(reg, "/i") {
				reg = fmt.Sprintf("(?i)%s", reg[1:len(reg)-2])
			} else {
				reg = reg[1 : len(reg)-1]
			}
		}
		r, err := regexp.Compile(reg)
		if err != nil {
			return fmt.Errorf("Error parsing regex: %s", err)
		}
		s.compiledRegex = r
	}
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
		s.SetDefaults()
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
		return time.Duration(0)
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
