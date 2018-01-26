package backup_util

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/gogo/protobuf/proto"
	internal "github.com/influxdata/influxdb/cmd/influxd/backup_util/internal"
	"github.com/influxdata/influxdb/services/snapshotter"
	"io/ioutil"
	"path/filepath"
)

//go:generate protoc --gogo_out=. internal/data.proto

const (
	// Suffix is a suffix added to the backup while it's in-process.
	Suffix = ".pending"

	// Metafile is the base name given to the metastore backups.
	Metafile = "meta"

	// BackupFilePattern is the beginning of the pattern for a backup
	// file. They follow the scheme <database>.<retention>.<shardID>.<increment>
	BackupFilePattern = "%s.%s.%05d"

	PortableFileNamePattern = "20060102T150405Z"
)

type PortablePacker struct {
	Data      []byte
	MaxNodeID uint64
}

func (ep PortablePacker) MarshalBinary() ([]byte, error) {
	ed := internal.PortableData{Data: ep.Data, MaxNodeID: &ep.MaxNodeID}
	return proto.Marshal(&ed)
}

func (ep *PortablePacker) UnmarshalBinary(data []byte) error {
	var pb internal.PortableData
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}

	ep.Data = pb.GetData()
	ep.MaxNodeID = pb.GetMaxNodeID()
	return nil
}

func GetMetaBytes(fname string) ([]byte, error) {
	f, err := os.Open(fname)
	if err != nil {
		return []byte{}, err
	}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, f); err != nil {
		return []byte{}, fmt.Errorf("copy: %s", err)
	}

	b := buf.Bytes()
	var i int

	// Make sure the file is actually a meta store backup file
	magic := binary.BigEndian.Uint64(b[:8])
	if magic != snapshotter.BackupMagicHeader {
		return []byte{}, fmt.Errorf("invalid metadata file")
	}
	i += 8

	// Size of the meta store bytes
	length := int(binary.BigEndian.Uint64(b[i : i+8]))
	i += 8
	metaBytes := b[i : i+length]

	return metaBytes, nil
}

// Manifest lists the meta and shard file information contained in the backup.
// If Limited is false, the manifest contains a full backup, otherwise
// it is a partial backup.
type Manifest struct {
	Meta    MetaEntry `json:"meta"`
	Limited bool      `json:"limited"`
	Files   []Entry   `json:"files"`

	// If limited is true, then one (or all) of the following fields will be set

	Database string `json:"database,omitempty"`
	Policy   string `json:"policy,omitempty"`
	ShardID  uint64 `json:"shard_id,omitempty"`
}

// Entry contains the data information for a backed up shard.
type Entry struct {
	Database     string `json:"database"`
	Policy       string `json:"policy"`
	ShardID      uint64 `json:"shardID"`
	FileName     string `json:"fileName"`
	Size         int64  `json:"size"`
	LastModified int64  `json:"lastModified"`
}

func (e *Entry) SizeOrZero() int64 {
	if e == nil {
		return 0
	}
	return e.Size
}

// MetaEntry contains the meta store information for a backup.
type MetaEntry struct {
	FileName string `json:"fileName"`
	Size     int64  `json:"size"`
}

// Size returns the size of the manifest.
func (m *Manifest) Size() int64 {
	if m == nil {
		return 0
	}

	size := m.Meta.Size

	for _, f := range m.Files {
		size += f.Size
	}
	return size
}

func (manifest *Manifest) Save(filename string) error {
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("create manifest: %v", err)
	}

	return ioutil.WriteFile(filename, b, 0600)
}

// LoadIncremental loads multiple manifest files from a given directory.
func LoadIncremental(dir string) (*MetaEntry, map[uint64]*Entry, error) {
	manifests, err := filepath.Glob(filepath.Join(dir, "*.manifest"))
	if err != nil {
		return nil, nil, err
	}
	shards := make(map[uint64]*Entry)

	if len(manifests) == 0 {
		return nil, shards, nil
	}

	sort.Sort(sort.Reverse(sort.StringSlice(manifests)))
	var metaEntry MetaEntry

	for _, fileName := range manifests {
		fi, err := os.Stat(fileName)
		if err != nil {
			return nil, nil, err
		}

		if fi.IsDir() {
			continue
		}

		f, err := os.Open(fileName)
		if err != nil {
			return nil, nil, err
		}

		var manifest Manifest
		err = json.NewDecoder(f).Decode(&manifest)
		f.Close()
		if err != nil {
			return nil, nil, fmt.Errorf("read manifest: %v", err)
		}

		// sorted (descending) above, so first manifest is most recent
		if metaEntry.FileName == "" {
			metaEntry = manifest.Meta
		}

		for i := range manifest.Files {
			sh := manifest.Files[i]
			if _, err := os.Stat(filepath.Join(dir, sh.FileName)); err != nil {
				continue
			}

			e := shards[sh.ShardID]
			if e == nil || sh.LastModified > e.LastModified {
				shards[sh.ShardID] = &sh
			}
		}
	}

	return &metaEntry, shards, nil
}

type CountingWriter struct {
	io.Writer
	Total int64 // Total # of bytes transferred
}

func (w *CountingWriter) Write(p []byte) (n int, err error) {
	n, err = w.Writer.Write(p)
	w.Total += int64(n)
	return
}

// retentionAndShardFromPath will take the shard relative path and split it into the
// retention policy name and shard ID. The first part of the path should be the database name.
func DBRetentionAndShardFromPath(path string) (db, retention, shard string, err error) {
	a := strings.Split(path, string(filepath.Separator))
	if len(a) != 3 {
		return "", "", "", fmt.Errorf("expected database, retention policy, and shard id in path: %s", path)
	}

	return a[0], a[1], a[2], nil
}
