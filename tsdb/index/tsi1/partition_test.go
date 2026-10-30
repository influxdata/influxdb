package tsi1_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
	"github.com/stretchr/testify/assert"
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

// openPartitionWithRetainedLog opens a partition holding one series in its
// active log file, retains the file set the way a series deletion iterator does,
// and compacts the log file. It returns once the compaction has swapped the log
// file out of the file set, along with a func releasing the retained set and
// the replaced path. Cleanup releases the set and closes the partition even if
// the test fails first; release may be called more than once.
func openPartitionWithRetainedLog(t *testing.T) (*Partition, func(), string) {
	t.Helper()
	sfile := MustOpenSeriesFile()
	t.Cleanup(func() { require.NoError(t, sfile.Close(), "closing series file") })

	p := MustOpenPartition(sfile.SeriesFile)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(p.Path()), "removing partition dir") })
	// Runs before the removal above. Close is a no-op if the test closed it.
	t.Cleanup(func() { require.NoError(t, p.Partition.Close(), "closing partition") })

	// Long age so writing a series does not auto-roll the active log file.
	p.Partition.SetMaxLogFileAge(time.Hour)
	_, err := p.CreateSeriesListIfNotExists(
		[][]byte{[]byte("cpu")},
		[]models.Tags{models.NewTags(map[string]string{"region": "east"})},
		notrack)
	require.NoError(t, err, "creating series")

	// Stand-in for the series iterator built by DeleteMeasurement/DeleteSeries.
	fs, err := p.RetainFileSet()
	require.NoError(t, err, "retaining file set")
	release := sync.OnceFunc(fs.Release)
	// Runs before the partition Close above, which would wait on this reader.
	t.Cleanup(release)
	files := fs.Files()
	require.Len(t, files, 1, "expected only the active log file")
	logPath := files[0].Path()
	require.Equal(t, tsi1.LogFileExt, filepath.Ext(logPath))

	p.Partition.SetMaxLogFileSize(1)
	p.Compact()
	// FileN changes when the active log rolls, before the compaction swap, so
	// wait for the old log to leave the file set instead.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		cur, err := p.RetainFileSet()
		if !assert.NoError(c, err, "retaining current file set") {
			return
		}
		defer cur.Release()
		for _, f := range cur.Files() {
			assert.NotEqual(c, logPath, f.Path(), "replaced log file still in the file set")
		}
	}, 10*time.Second, time.Millisecond, "log compaction never swapped in its index file")
	return p, release, logPath
}

// TestPartition_Wait_NotBlockedByReader is the regression test for a deadlock
// between series deletion and compaction: deletion holds a file set reference
// and then calls Wait(), while the compaction it waits on blocks closing the
// file that reference pins.
func TestPartition_Wait_NotBlockedByReader(t *testing.T) {
	p, release, logPath := openPartitionWithRetainedLog(t)

	p.DisableCompactions()
	waited := make(chan struct{})
	go func() {
		defer close(waited)
		p.Wait()
	}()
	select {
	case <-waited:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "Partition.Wait() blocked on a file set held by a reader")
	}

	_, err := os.Stat(logPath)
	require.NoError(t, err, "replaced log file removed while a reader still holds it")

	release()
	require.Eventually(t, func() bool {
		_, err := os.Stat(logPath)
		return errors.Is(err, os.ErrNotExist)
	}, 10*time.Second, time.Millisecond, "replaced log file not removed after the reader released it")

	p.EnableCompactions()
	require.NoError(t, p.Partition.Close(), "closing partition")
}

// TestPartition_Close_WaitsForReclaim checks that Close still waits for
// compaction goroutines to close and remove the files they replaced.
func TestPartition_Close_WaitsForReclaim(t *testing.T) {
	p, release, logPath := openPartitionWithRetainedLog(t)

	closed := make(chan error, 1)
	go func() { closed <- p.Partition.Close() }()

	select {
	case err := <-closed:
		require.FailNow(t, "Partition.Close() returned while a replaced file was still referenced", "err: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	_, err := os.Stat(logPath)
	require.NoError(t, err, "replaced log file removed while a reader still holds it")

	release()
	select {
	case err := <-closed:
		require.NoError(t, err, "closing partition")
	case <-time.After(10 * time.Second):
		require.FailNow(t, "Partition.Close() did not return after the reader released its file set")
	}
	_, err = os.Stat(logPath)
	require.ErrorIs(t, err, os.ErrNotExist, "replaced log file still on disk after Close")
}

// TestPartition_Wait_ReadersAndDeleters_Stress runs writers that force a log
// compaction on every series, readers that hold file sets, and deleters that
// hold a file set and then Wait(), which is the deletion shape that deadlocked.
func TestPartition_Wait_ReadersAndDeleters_Stress(t *testing.T) {
	const (
		writerN       = 2
		readerN       = 4
		deleterN      = 2
		seriesPerWrtr = 200
		total         = writerN + readerN + deleterN
	)

	sfile := MustOpenSeriesFile()
	t.Cleanup(func() { require.NoError(t, sfile.Close(), "closing series file") })
	p := MustOpenPartition(sfile.SeriesFile)
	stop := make(chan struct{})
	stopWorkers := sync.OnceFunc(func() { close(stop) })
	// Close is a no-op if the test closed it. It is bounded so a deadlocked
	// partition fails cleanup instead of hanging the package, and the directory
	// is only removed once nothing can still be writing to it.
	t.Cleanup(func() {
		stopWorkers()
		closed := make(chan error, 1)
		go func() { closed <- p.Partition.Close() }()
		select {
		case err := <-closed:
			require.NoError(t, err, "closing partition")
			require.NoError(t, os.RemoveAll(p.Path()), "removing partition dir")
		case <-time.After(10 * time.Second):
			require.FailNow(t, "partition Close did not return; leaving "+p.Path()+" in place")
		}
	})
	p.Partition.SetMaxLogFileAge(time.Hour)
	p.Partition.SetMaxLogFileSize(1)

	// Create test data upfront.
	names := make([][][]byte, writerN)
	tags := make([][]models.Tags, writerN)
	for w := range writerN {
		for i := range seriesPerWrtr {
			names[w] = append(names[w], []byte("cpu"))
			tags[w] = append(tags[w], models.NewTags(map[string]string{
				"host": fmt.Sprintf("w%d-h%d", w, i),
			}))
		}
	}

	var concurrency, maxConcurrency atomic.Int64
	var arrived sync.WaitGroup
	proceed := make(chan struct{})
	arrived.Add(total)
	go func() {
		arrived.Wait()
		close(proceed)
	}()
	enter := func() {
		c := concurrency.Add(1)
		for old := maxConcurrency.Load(); c > old && !maxConcurrency.CompareAndSwap(old, c); old = maxConcurrency.Load() {
		}
		arrived.Done()
		<-proceed
	}

	errs := make(chan error, total)
	var writers, others sync.WaitGroup
	for w := range writerN {
		writers.Add(1)
		go func() {
			defer writers.Done()
			enter()
			defer concurrency.Add(-1)
			for i := range seriesPerWrtr {
				if _, err := p.CreateSeriesListIfNotExists(names[w][i:i+1], tags[w][i:i+1], notrack); err != nil {
					errs <- fmt.Errorf("writer %d series %d: %w", w, i, err)
					return
				}
			}
		}()
	}
	for r := range readerN {
		others.Add(1)
		go func() {
			defer others.Done()
			enter()
			defer concurrency.Add(-1)
			for {
				select {
				case <-stop:
					return
				default:
				}
				fs, err := p.RetainFileSet()
				if err != nil {
					errs <- fmt.Errorf("reader %d: %w", r, err)
					return
				}
				time.Sleep(50 * time.Microsecond)
				fs.Release()
			}
		}()
	}
	for d := range deleterN {
		others.Add(1)
		go func() {
			defer others.Done()
			enter()
			defer concurrency.Add(-1)
			for {
				select {
				case <-stop:
					return
				default:
				}
				fs, err := p.RetainFileSet()
				if err != nil {
					errs <- fmt.Errorf("deleter %d: %w", d, err)
					return
				}
				p.DisableCompactions()
				p.Wait()
				fs.Release()
				p.EnableCompactions()
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		writers.Wait()
		stopWorkers()
		others.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Minute):
		require.FailNow(t, "deadlock: writers, readers and deleters did not finish")
	}
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	t.Logf("max concurrency: %d", maxConcurrency.Load())
	require.Equal(t, int64(total), maxConcurrency.Load(), "goroutines did not all overlap")

	// Quiesce before Close: a compaction interrupted by Close leaves its partial
	// output on disk until the next Open sweeps it, which is not what this
	// checks. Close itself must still wait for replaced files to be removed.
	p.DisableCompactions()
	p.Wait()
	require.NoError(t, p.Partition.Close(), "closing partition")

	// Every file left on disk must be named by the manifest.
	m, _, err := tsi1.ReadManifestFile(filepath.Join(p.Path(), tsi1.ManifestFileName))
	require.NoError(t, err, "reading manifest")
	entries, err := os.ReadDir(p.Path())
	require.NoError(t, err, "reading partition dir")
	var onDisk []string
	for _, e := range entries {
		if e.Name() != tsi1.ManifestFileName {
			onDisk = append(onDisk, e.Name())
		}
	}
	require.ElementsMatch(t, m.Files, onDisk, "files on disk do not match the manifest")
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
