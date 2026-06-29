package tsi1_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
	"github.com/stretchr/testify/require"
)

func TestPartition_Open(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	// Opening a fresh index should set the MANIFEST version to current version.
	p := NewPartition(sfile.SeriesFile)
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
			p = NewPartition(sfile.SeriesFile)
			// Manually create a MANIFEST file for an incompatible index version.
			mpath := filepath.Join(p.Path(), tsi1.ManifestFileName)
			m := tsi1.NewManifest(mpath)
			m.Levels = nil
			m.Version = v // Set example MANIFEST version.
			if _, err := m.Write(); err != nil {
				t.Fatal(err)
			}

			// Log the MANIFEST file.
			data, err := os.ReadFile(mpath)
			if err != nil {
				panic(err)
			}
			t.Logf("Incompatible MANIFEST: %s", data)

			// Opening this index should return an error because the MANIFEST has an
			// incompatible version.
			err = p.Open()
			if !errors.Is(err, tsi1.ErrIncompatibleVersion) {
				p.Close()
				t.Fatalf("got error %v, expected %v", err, tsi1.ErrIncompatibleVersion)
			}
		})
	}
}

func TestPartition_Manifest(t *testing.T) {
	t.Run("current MANIFEST", func(t *testing.T) {
		sfile := MustOpenSeriesFile()
		t.Cleanup(func() {
			if err := sfile.Close(); err != nil {
				t.Fatalf("error closing series file %v", err)
			}
		})

		p := MustOpenPartition(sfile.SeriesFile)
		t.Cleanup(func() {
			if err := p.Close(); err != nil {
				t.Fatalf("error closing partition %v", err)
			}
		})

		if got, exp := p.Manifest().Version, tsi1.Version; got != exp {
			t.Fatalf("got MANIFEST version %d, expected %d", got, exp)
		}
	})
}

var badManifestPath string = filepath.Join(os.DevNull, tsi1.ManifestFileName)

func TestPartition_Manifest_Write_Fail(t *testing.T) {
	t.Run("write MANIFEST", func(t *testing.T) {
		m := tsi1.NewManifest(badManifestPath)
		_, err := m.Write()
		if !errors.Is(err, syscall.ENOTDIR) {
			t.Fatalf("expected: syscall.ENOTDIR, got %T: %v", err, err)
		}
	})
}

func TestPartition_PrependLogFile_Write_Fail(t *testing.T) {
	t.Run("write MANIFEST", func(t *testing.T) {
		sfile := MustOpenSeriesFile()
		t.Cleanup(func() {
			if err := sfile.Close(); err != nil {
				t.Fatalf("error closing series file %v", err)
			}
		})
		p := MustOpenPartition(sfile.SeriesFile)
		t.Cleanup(func() {
			if err := p.Close(); err != nil {
				t.Fatalf("error closing partition: %v", err)
			}
		})
		p.Partition.SetMaxLogFileSize(-1)
		fileN := p.FileN()
		p.CheckLogFile()
		if fileN >= p.FileN() {
			t.Fatalf("manifest write prepending log file should have succeeded but number of files did not change correctly: expected more than %d files, got %d files", fileN, p.FileN())
		}
		p.SetManifestPathForTest(badManifestPath)
		fileN = p.FileN()
		p.CheckLogFile()
		if fileN != p.FileN() {
			t.Fatalf("manifest write prepending log file should have failed, but number of files changed: expected %d files, got %d files", fileN, p.FileN())
		}
	})
}

func TestPartition_Compact_Write_Fail(t *testing.T) {
	t.Run("write MANIFEST", func(t *testing.T) {
		sfile := MustOpenSeriesFile()
		t.Cleanup(func() {
			if err := sfile.Close(); err != nil {
				t.Fatalf("error closing series file %v", err)
			}
		})

		p := MustOpenPartition(sfile.SeriesFile)
		t.Cleanup(func() { require.NoError(t, p.Close(), "error closing partition") })

		// Long age so writing a series does not auto-roll the active log file
		// (a bare Partition has maxLogFileAge == 0, which compacts any non-empty log).
		p.Partition.SetMaxLogFileAge(time.Hour)

		// Seed one series so the active log file is non-empty.
		_, err := p.CreateSeriesListIfNotExists(
			[][]byte{[]byte("cpu")},
			[]models.Tags{models.NewTags(map[string]string{"region": "east"})},
			notrack)
		require.NoError(t, err, "creating series")

		// Size threshold 1: the populated log needs compaction, but a freshly
		// rolled EMPTY log (size 0 < 1) does not, so the async re-trigger chain
		// settles and the count cannot grow past fileN+1 under any interleaving.
		p.Partition.SetMaxLogFileSize(1)
		fileN := p.FileN()
		p.Compact()
		p.Wait() // settles; cannot re-trigger because the rolled log is empty
		require.Equal(t, fileN+1, p.FileN(),
			"manifest write in compaction should have succeeded and changed the file count")

		// Part 2: a failing MANIFEST write during the roll must roll back, leaving
		// the count unchanged. Raise the threshold so the next write doesn't roll,
		// then shrink it again so the populated log needs compaction.
		p.Partition.SetMaxLogFileSize(tsdb.DefaultMaxIndexLogFileSize)
		_, err = p.CreateSeriesListIfNotExists(
			[][]byte{[]byte("mem")},
			[]models.Tags{models.NewTags(map[string]string{"region": "west"})},
			notrack)
		require.NoError(t, err, "creating second series")
		p.Partition.SetMaxLogFileSize(1)

		p.SetManifestPathForTest(badManifestPath)
		fileN = p.FileN()
		p.Compact()
		p.Wait()
		require.Equal(t, fileN, p.FileN(),
			"failed manifest write must not change the file count")
	})
}

// Partition is a test wrapper for tsi1.Partition.
type Partition struct {
	*tsi1.Partition
}

// NewPartition returns a new instance of Partition at a temporary path.
func NewPartition(sfile *tsdb.SeriesFile) *Partition {
	return &Partition{Partition: tsi1.NewPartition(sfile, MustTempPartitionDir())}
}

// MustOpenPartition returns a new, open index. Panic on error.
func MustOpenPartition(sfile *tsdb.SeriesFile) *Partition {
	p := NewPartition(sfile)
	if err := p.Open(); err != nil {
		panic(err)
	}
	return p
}

// Close closes and removes the index directory.
func (p *Partition) Close() error {
	defer os.RemoveAll(p.Path())
	return p.Partition.Close()
}

// Reopen closes and opens the index.
func (p *Partition) Reopen() error {
	if err := p.Partition.Close(); err != nil {
		return err
	}

	sfile, path := p.SeriesFile(), p.Path()
	p.Partition = tsi1.NewPartition(sfile, path)
	return p.Open()
}
