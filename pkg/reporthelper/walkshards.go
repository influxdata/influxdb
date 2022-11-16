// Package reporthelper reports statistics about TSM files.
package reporthelper

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
)

func IsShardDir(dir string) error {
	name := filepath.Base(dir)
	if id, err := strconv.Atoi(name); err != nil || id < 1 {
		return fmt.Errorf("not a valid shard dir: %v", dir)
	}

	return nil
}

func WalkShardDirs(root string, fn func(db, rp, id, path string) error) error {
	type location struct {
		db, rp, id, path string
	}

	var dirs []location
	if err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if filepath.Ext(info.Name()) == "."+tsm1.TSMFileExtension {
			shardDir := filepath.Dir(path)

			if err := IsShardDir(shardDir); err != nil {
				return err
			}
			absPath, err := filepath.Abs(path)
			if err != nil {
				return err
			}
			parts := strings.Split(absPath, string(filepath.Separator))
			db, rp, id := parts[len(parts)-4], parts[len(parts)-3], parts[len(parts)-2]
			dirs = append(dirs, location{db: db, rp: rp, id: id, path: path})
			return nil
		}
		return nil
	}); err != nil {
		return err
	}

	sort.Slice(dirs, func(i, j int) bool {
		a, _ := strconv.Atoi(dirs[i].id)
		b, _ := strconv.Atoi(dirs[j].id)
		return a < b
	})

	for _, shard := range dirs {
		if err := fn(shard.db, shard.rp, shard.id, shard.path); err != nil {
			return err
		}
	}
	return nil
}
