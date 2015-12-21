package main

import (
	"os"
	"path"
	"path/filepath"
	"sort"
)

const (
	b1 = iota
	bz1
	tsm1
)

type EngineFormat int

func (e EngineFormat) String() string {
	switch e {
	case tsm1:
		return "tsm1"
	case b1:
		return "b1"
	case bz1:
		return "bz1"
	default:
		panic("unrecognized shard engine format")
	}
}

// ShardInfo is the description of a shard on disk.
type ShardInfo struct {
	Database        string
	RetentionPolicy string
	Path            string
	Format          EngineFormat
	Size            int64
}

func (s *ShardInfo) FormatAsString() string {
	return s.Format.String()
}

type ShardInfos []*ShardInfo

func (s ShardInfos) Len() int      { return len(s) }
func (s ShardInfos) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s ShardInfos) Less(i, j int) bool {
	if s[i].Database == s[j].Database {
		if s[i].RetentionPolicy == s[j].RetentionPolicy {
			return s[i].Path < s[i].Path
		} else {
			return s[i].RetentionPolicy < s[j].RetentionPolicy
		}
	}
	return s[i].Database < s[j].Database
}

// Databases returns the sorted unique set of databases for the shards.
func (s ShardInfos) Databases() []string {
	dbm := make(map[string]bool)
	for _, ss := range s {
		dbm[ss.Database] = true
	}

	var dbs []string
	for k, _ := range dbm {
		dbs = append(dbs, k)
	}
	sort.Strings(dbs)
	return dbs
}

// Filter returns a copy of the ShardInfos, with shards of the given
// format removed.
func (s ShardInfos) Filter(fmt EngineFormat) ShardInfos {
	var a ShardInfos
	for _, si := range s {
		if si.Format != fmt {
			a = append(a, si)
		}
	}
	return a
}

// Database represents an entire database on disk.
type Database struct {
	path string
}

// NewDatabase creates a database instance using data at path.
func NewDatabase(path string) *Database {
	return &Database{path: path}
}

// Name returns the name of the database.
func (d *Database) Name() string {
	return path.Base(d.path)
}

// Path returns the path to the database.
func (d *Database) Path() string {
	return d.path
}

// Shards returns information for every shard in the database.
func (d *Database) Shards() ([]*ShardInfo, error) {
	fd, err := os.Open(d.path)
	if err != nil {
		return nil, err
	}

	// Get each retention policy.
	rps, err := fd.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	// Process each retention policy.
	var shardInfos []*ShardInfo
	for _, rp := range rps {
		rpfd, err := os.Open(filepath.Join(d.path, rp))
		if err != nil {
			return nil, err
		}

		// Process each shard
		shards, err := rpfd.Readdirnames(-1)
		for _, sh := range shards {
			fmt, sz, err := shardFormat(filepath.Join(d.path, rp, sh))
			if err != nil {
				return nil, err
			}

			si := &ShardInfo{
				Database:        d.Name(),
				RetentionPolicy: path.Base(rp),
				Path:            sh,
				Format:          fmt,
				Size:            sz,
			}
			shardInfos = append(shardInfos, si)
		}
	}

	sort.Sort(ShardInfos(shardInfos))
	return shardInfos, nil
}
