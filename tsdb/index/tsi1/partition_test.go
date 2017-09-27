package tsi1_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

func TestPartition_Open(t *testing.T) {
	// Opening a fresh index should set the MANIFEST version to current version.
	p := NewPartition()
	t.Run("open new index", func(t *testing.T) {
		if err := p.Open(); err != nil {
			t.Fatal(err)
		}

		// Check version set appropriately.
		if got, exp := p.Manifest().Version, 1; got != exp {
			t.Fatalf("got index version %d, expected %d", got, exp)
		}
	})

	// Reopening an open index should return an error.
	t.Run("reopen open index", func(t *testing.T) {
		err := p.Open()
		if err == nil {
			p.Close()
			t.Fatal("didn't get an error on reopen, but expected one")
		}
		p.Close()
	})

	// Opening an incompatible index should return an error.
	incompatibleVersions := []int{-1, 0, 2}
	for _, v := range incompatibleVersions {
		t.Run(fmt.Sprintf("incompatible index version: %d", v), func(t *testing.T) {
			p = NewPartition()
			// Manually create a MANIFEST file for an incompatible index version.
			mpath := filepath.Join(p.Path, tsi1.ManifestFileName)
			m := tsi1.NewManifest()
			m.Levels = nil
			m.Version = v // Set example MANIFEST version.
			if err := tsi1.WriteManifestFile(mpath, m); err != nil {
				t.Fatal(err)
			}

			// Log the MANIFEST file.
			data, err := ioutil.ReadFile(mpath)
			if err != nil {
				panic(err)
			}
			t.Logf("Incompatible MANIFEST: %s", data)

			// Opening this index should return an error because the MANIFEST has an
			// incompatible version.
			err = p.Open()
			if err != tsi1.ErrIncompatibleVersion {
				p.Close()
				t.Fatalf("got error %v, expected %v", err, tsi1.ErrIncompatibleVersion)
			}
		})
	}
}

func TestPartition_Manifest(t *testing.T) {
	t.Run("current MANIFEST", func(t *testing.T) {
		p := MustOpenPartition()
		if got, exp := p.Manifest().Version, tsi1.Version; got != exp {
			t.Fatalf("got MANIFEST version %d, expected %d", got, exp)
		}
	})
}

// Partition is a test wrapper for tsi1.Partition.
type Partition struct {
	*tsi1.Partition
}

// NewPartition returns a new instance of Partition at a temporary path.
func NewPartition() *Partition {
	p := &Partition{Partition: tsi1.NewPartition()}
	p.Path = MustTempDir()
	return p
}

// MustOpenPartition returns a new, open index. Panic on error.
func MustOpenPartition() *Partition {
	p := NewPartition()
	if err := p.Open(); err != nil {
		panic(err)
	}
	return p
}

// Close closes and removes the index directory.
func (p *Partition) Close() error {
	defer os.RemoveAll(p.Path)
	return p.Partition.Close()
}

// Reopen closes and opens the index.
func (p *Partition) Reopen() error {
	if err := p.Partition.Close(); err != nil {
		return err
	}

	path := p.Path
	p.Partition = tsi1.NewPartition()
	p.Path = path
	return p.Open()
}
