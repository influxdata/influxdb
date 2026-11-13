package tsi1_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/index/tsi1"
	"github.com/stretchr/testify/require"
)

func TestPartition_Open(t *testing.T) {
	sfile := MustOpenSeriesFile(t)
	defer sfile.Close()

	// Opening a fresh index should set the MANIFEST version to current version.
	p := NewPartition(t, sfile.SeriesFile)
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
			p = NewPartition(t, sfile.SeriesFile)
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
		sfile := MustOpenSeriesFile(t)
		t.Cleanup(func() { sfile.Close() })

		p := MustOpenPartition(t, sfile.SeriesFile)
		t.Cleanup(func() { p.Close() })

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
		sfile := MustOpenSeriesFile(t)
		t.Cleanup(func() { sfile.Close() })

		p := MustOpenPartition(t, sfile.SeriesFile)
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
		sfile := MustOpenSeriesFile(t)
		t.Cleanup(func() { sfile.Close() })

		p := MustOpenPartition(t, sfile.SeriesFile)
		t.Cleanup(func() { require.NoError(t, p.Close(), "error closing partition") })

		// Long age so writing a series does not auto-roll the active log file
		// (a bare Partition has maxLogFileAge == 0, which compacts any non-empty log).
		p.Partition.SetMaxLogFileAge(time.Hour)

		// Seed one series so the active log file is non-empty.
		_, err := p.CreateSeriesListIfNotExists(
			[][]byte{[]byte("cpu")},
			[]models.Tags{models.NewTags(map[string]string{"region": "east"})})
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
			[]models.Tags{models.NewTags(map[string]string{"region": "west"})})
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
func NewPartition(tb testing.TB, sfile *tsdb.SeriesFile) *Partition {
	return &Partition{Partition: tsi1.NewPartition(sfile, MustTempPartitionDir(tb))}
}

// MustOpenPartition returns a new, open index. Panic on error.
func MustOpenPartition(tb testing.TB, sfile *tsdb.SeriesFile) *Partition {
	p := NewPartition(tb, sfile)
	if err := p.Open(); err != nil {
		panic(err)
	}
	return p
}

// Close closes and removes the index directory.
func (p *Partition) Close() error {
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

// Regression test for the delete vs log-compaction deadlock: a delete retains
// the file set through its series iterator and then blocks in Wait() for
// in-progress compactions, while the compaction blocked in LogFile.Close()
// waiting for the iterator's reference. Wait() must return once the
// compaction is logically complete (manifest swapped), regardless of the
// outstanding reference.
func TestPartition_CompactLogFile_HeldReferenceDoesNotBlockWait(t *testing.T) {
	sfile := MustOpenSeriesFile(t)
	t.Cleanup(func() { sfile.Close() })

	p := MustOpenPartition(t, sfile.SeriesFile)
	t.Cleanup(func() { require.NoError(t, p.Close(), "error closing partition") })

	// Long age so writing a series does not auto-roll the active log file.
	p.Partition.SetMaxLogFileAge(time.Hour)

	// Seed one series so the active log file is non-empty.
	_, err := p.CreateSeriesListIfNotExists(
		[][]byte{[]byte("cpu")},
		[]models.Tags{models.NewTags(map[string]string{"region": "east"})})
	require.NoError(t, err, "creating series")

	// Threshold of 1: the populated log needs compaction, but the freshly
	// rolled empty log does not, so the re-trigger chain settles.
	p.Partition.SetMaxLogFileSize(1)

	// Hold a reference to the current file set, as a delete's series iterator
	// would. The reference covers the log file about to be compacted.
	fs, err := p.Partition.RetainFileSet()
	require.NoError(t, err, "retaining file set")
	released := false
	defer func() {
		if !released {
			fs.Release()
		}
	}()

	// Kick off the log compaction in the background.
	p.Compact()

	// This is what the delete path does next: wait for in-progress
	// compactions. It must not block on the reference we hold.
	done := make(chan struct{})
	go func() {
		p.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		// Release first so the stuck compaction can finish and partition
		// cleanup does not hang.
		fs.Release()
		released = true
		t.Fatal("Wait() blocked by a compaction waiting on a held file reference (deadlock)")
	}

	// Once the reference is released, the swapped-out log file must be
	// closed and removed, leaving only the fresh active log file.
	fs.Release()
	released = true
	require.Eventually(t, func() bool {
		logs, err := filepath.Glob(filepath.Join(p.Path(), "*"+tsi1.LogFileExt))
		return err == nil && len(logs) == 1
	}, 30*time.Second, 10*time.Millisecond, "compacted log file was not removed after its last reference was released")
}

// The first DisableCompactions call must interrupt in-flight compactions so
// that a delete waiting in Wait() is not stuck behind a full compaction.
// Nested disables must not close the replacement channel.
func TestPartition_DisableCompactions_InterruptsInflight(t *testing.T) {
	sfile := MustOpenSeriesFile(t)
	t.Cleanup(func() { sfile.Close() })

	p := MustOpenPartition(t, sfile.SeriesFile)
	t.Cleanup(func() { require.NoError(t, p.Close(), "error closing partition") })

	first := p.Partition.CompactionInterruptForTest()

	p.Partition.DisableCompactions()
	select {
	case <-first:
	default:
		t.Fatal("first DisableCompactions did not close the interrupt channel")
	}

	second := p.Partition.CompactionInterruptForTest()
	require.NotEqual(t, first, second, "a fresh interrupt channel should be installed")

	// A nested disable is not the first disabler and must leave the
	// replacement channel open.
	p.Partition.DisableCompactions()
	select {
	case <-second:
		t.Fatal("nested DisableCompactions closed the replacement interrupt channel")
	default:
	}

	p.Partition.EnableCompactions()
	p.Partition.EnableCompactions()

	// Re-enabled: disabling again interrupts via the current channel.
	p.Partition.DisableCompactions()
	select {
	case <-second:
	default:
		t.Fatal("DisableCompactions after re-enable did not close the interrupt channel")
	}
	p.Partition.EnableCompactions()
}
