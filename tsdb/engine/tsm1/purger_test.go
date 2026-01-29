package tsm1

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// mockPurgerTSMFile implements TSMFile for testing the purger.
// It embeds mockTSMFile to inherit stub implementations for unused methods.
type mockPurgerTSMFile struct {
	mockTSMFile // provides stub implementations for unused interface methods
	path        string
	inUse       atomic.Bool
	closed      atomic.Bool
	removed     atomic.Bool
	closeCh     chan struct{} // closed when Close() is called
	removeCh    chan struct{} // closed when Remove() is called
}

func newMockPurgerTSMFile(path string, inUse bool) *mockPurgerTSMFile {
	m := &mockPurgerTSMFile{
		path:     path,
		closeCh:  make(chan struct{}),
		removeCh: make(chan struct{}),
	}
	m.inUse.Store(inUse)
	return m
}

func (m *mockPurgerTSMFile) Path() string  { return m.path }
func (m *mockPurgerTSMFile) InUse() bool   { return m.inUse.Load() }
func (m *mockPurgerTSMFile) Ref()          { m.inUse.Store(true) }
func (m *mockPurgerTSMFile) Unref()        { m.inUse.Store(false) }
func (m *mockPurgerTSMFile) Closed() bool  { return m.closed.Load() }
func (m *mockPurgerTSMFile) Removed() bool { return m.removed.Load() }

func (m *mockPurgerTSMFile) Close() error {
	m.closed.Store(true)
	close(m.closeCh)
	return nil
}

func (m *mockPurgerTSMFile) Remove() error {
	m.removed.Store(true)
	close(m.removeCh)
	return nil
}

func newTestPurger() *purger {
	return &purger{
		logger: zap.NewNop(),
	}
}

// TestPurger_Add_Empty verifies that adding an empty slice is a no-op.
func TestPurger_Add_Empty(t *testing.T) {
	p := newTestPurger()

	p.add(nil)
	require.Equal(t, 0, p.files.Len())

	p.add([]TSMFile{})
	require.Equal(t, 0, p.files.Len())
}

// TestPurger_Add_PurgesNotInUse verifies that files not in use are purged.
func TestPurger_Add_PurgesNotInUse(t *testing.T) {
	p := newTestPurger()

	f1 := newMockPurgerTSMFile("file1.tsm", false) // not in use
	f2 := newMockPurgerTSMFile("file2.tsm", false) // not in use

	p.add([]TSMFile{f1, f2})

	// Wait for both files to be removed
	select {
	case <-f1.removeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for file1 to be removed")
	}
	select {
	case <-f2.removeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for file2 to be removed")
	}

	require.True(t, f1.Closed(), "file1 should be closed")
	require.True(t, f1.Removed(), "file1 should be removed")
	require.True(t, f2.Closed(), "file2 should be closed")
	require.True(t, f2.Removed(), "file2 should be removed")
}

// TestPurger_Add_WaitsForInUse verifies that files in use are not purged until released.
func TestPurger_Add_WaitsForInUse(t *testing.T) {
	p := newTestPurger()

	f := newMockPurgerTSMFile("file.tsm", true) // in use

	p.add([]TSMFile{f})

	// Give the purger time to attempt purging
	time.Sleep(100 * time.Millisecond)

	require.False(t, f.Closed(), "file should not be closed while in use")
	require.False(t, f.Removed(), "file should not be removed while in use")

	// Release the file
	f.Unref()

	// Wait for the file to be removed
	select {
	case <-f.removeCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for file to be removed after release")
	}

	require.True(t, f.Closed(), "file should be closed after release")
	require.True(t, f.Removed(), "file should be removed after release")
}

// TestPurger_RaceCondition_AddDuringExit_Stressed is an aggressive
// race condition test that uses goroutines to increase contention.
func TestPurger_RaceCondition_AddDuringExit_Stressed(t *testing.T) {
	p := newTestPurger()

	const numFiles = 100
	files := make([]*mockPurgerTSMFile, numFiles)
	for i := range numFiles {
		files[i] = newMockPurgerTSMFile(fmt.Sprintf("file%d.tsm", i), false)
	}

	// Use RWMutex to synchronize goroutine start for maximum concurrency
	var mu sync.RWMutex
	var concurrency, maxConcurrency atomic.Int64

	// Add files from multiple goroutines
	var wg sync.WaitGroup
	mu.Lock()
	for i := range numFiles {
		wg.Add(1)
		go func(idx int) {
			mu.RLock()
			defer mu.RUnlock()
			defer wg.Done()
			c := concurrency.Add(1)
			if old := maxConcurrency.Load(); c > old {
				maxConcurrency.CompareAndSwap(old, c)
			}
			p.add([]TSMFile{files[idx]})
			concurrency.Add(-1)
		}(i)
	}
	mu.Unlock() // Release to start all goroutines simultaneously
	wg.Wait()

	t.Logf("max concurrency: %d", maxConcurrency.Load())

	// All files must be purged
	for i, f := range files {
		select {
		case <-f.removeCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout waiting for file %d to be removed", i)
		}
	}
}

// TestPurger_ConcurrentAdd verifies that concurrent calls to add() work correctly.
func TestPurger_ConcurrentAdd(t *testing.T) {
	p := newTestPurger()

	const numGoroutines = 10
	const filesPerGoroutine = 10
	const numFiles = numGoroutines * filesPerGoroutine

	// Create all files upfront
	files := make([]*mockPurgerTSMFile, numFiles)
	for i := range numFiles {
		files[i] = newMockPurgerTSMFile(fmt.Sprintf("file%d.tsm", i), false)
	}

	// Use RWMutex to synchronize goroutine start for maximum concurrency
	var mu sync.RWMutex
	var concurrency, maxConcurrency atomic.Int64

	// Add files from multiple goroutines concurrently
	var wg sync.WaitGroup
	mu.Lock()
	for g := range numGoroutines {
		wg.Add(1)
		go func() {
			mu.RLock()
			defer mu.RUnlock()
			defer wg.Done()
			for i := range filesPerGoroutine {
				c := concurrency.Add(1)
				if old := maxConcurrency.Load(); c > old {
					maxConcurrency.CompareAndSwap(old, c)
				}
				p.add([]TSMFile{files[g*filesPerGoroutine+i]})
				concurrency.Add(-1)
			}
		}()
	}
	mu.Unlock() // Release to start all goroutines simultaneously
	wg.Wait()

	t.Logf("max concurrency: %d", maxConcurrency.Load())

	// All files must be purged
	for i, f := range files {
		select {
		case <-f.removeCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout waiting for file %d to be removed", i)
		}
	}
}

// TestPurger_ConcurrentAdd_MixedInUse verifies concurrent adds with a mix of
// in-use and not-in-use files.
func TestPurger_ConcurrentAdd_MixedInUse(t *testing.T) {
	p := newTestPurger()

	const numFiles = 50

	// Create all files upfront, alternating between in-use and not-in-use
	inUseFiles := make([]*mockPurgerTSMFile, 0, numFiles/2)
	notInUseFiles := make([]*mockPurgerTSMFile, 0, numFiles/2)
	allFiles := make([]*mockPurgerTSMFile, numFiles)
	for i := range numFiles {
		inUse := i%2 == 0
		f := newMockPurgerTSMFile(fmt.Sprintf("file%d.tsm", i), inUse)
		allFiles[i] = f
		if inUse {
			inUseFiles = append(inUseFiles, f)
		} else {
			notInUseFiles = append(notInUseFiles, f)
		}
	}

	// Use RWMutex to synchronize goroutine start for maximum concurrency
	var mu sync.RWMutex
	var concurrency, maxConcurrency atomic.Int64

	var wg sync.WaitGroup
	mu.Lock()
	for i := range numFiles {
		wg.Add(1)
		go func(idx int) {
			mu.RLock()
			defer mu.RUnlock()
			defer wg.Done()
			c := concurrency.Add(1)
			if old := maxConcurrency.Load(); c > old {
				maxConcurrency.CompareAndSwap(old, c)
			}
			p.add([]TSMFile{allFiles[idx]})
			concurrency.Add(-1)
		}(i)
	}
	mu.Unlock() // Release to start all goroutines simultaneously
	wg.Wait()

	t.Logf("max concurrency: %d", maxConcurrency.Load())

	// Not-in-use files should be purged
	for i, f := range notInUseFiles {
		select {
		case <-f.removeCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout waiting for not-in-use file %d to be removed", i)
		}
	}

	// In-use files should not be purged yet
	for i, f := range inUseFiles {
		require.False(t, f.Removed(), "in-use file %d should not be removed yet", i)
	}

	// Release all in-use files
	for _, f := range inUseFiles {
		f.Unref()
	}

	// Now all in-use files should be purged
	for i, f := range inUseFiles {
		select {
		case <-f.removeCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout waiting for released file %d to be removed", i)
		}
	}
}

// TestPurger_ConcurrentAdd_WhilePurging verifies that files added while purge is
// actively processing other files are eventually purged.
func TestPurger_ConcurrentAdd_WhilePurging(t *testing.T) {
	p := newTestPurger()

	const numHold = 5
	const numAdditional = 20

	// Create all files upfront
	holdFiles := make([]*mockPurgerTSMFile, numHold)
	for i := range numHold {
		holdFiles[i] = newMockPurgerTSMFile(fmt.Sprintf("hold%d.tsm", i), true)
	}
	additionalFiles := make([]*mockPurgerTSMFile, numAdditional)
	for i := range numAdditional {
		additionalFiles[i] = newMockPurgerTSMFile(fmt.Sprintf("additional%d.tsm", i), false)
	}

	// Add hold files to start the purger
	p.add(castPurgerMocksToTSMFiles(holdFiles))

	// Give the purger time to start
	time.Sleep(50 * time.Millisecond)

	// Use RWMutex to synchronize goroutine start for maximum concurrency
	var mu sync.RWMutex
	var concurrency, maxConcurrency atomic.Int64

	// Add additional files concurrently while the purger is running
	var wg sync.WaitGroup
	mu.Lock()
	for i := range numAdditional {
		wg.Add(1)
		go func(idx int) {
			mu.RLock()
			defer mu.RUnlock()
			defer wg.Done()
			c := concurrency.Add(1)
			if old := maxConcurrency.Load(); c > old {
				maxConcurrency.CompareAndSwap(old, c)
			}
			p.add([]TSMFile{additionalFiles[idx]})
			concurrency.Add(-1)
		}(i)
	}
	mu.Unlock() // Release to start all goroutines simultaneously
	wg.Wait()

	t.Logf("max concurrency: %d", maxConcurrency.Load())

	// Additional files should be purged even though hold files are still in use
	for i, f := range additionalFiles {
		select {
		case <-f.removeCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout waiting for additional file %d to be removed", i)
		}
	}

	// Release hold files
	for _, f := range holdFiles {
		f.Unref()
	}

	// Hold files should now be purged
	for i, f := range holdFiles {
		select {
		case <-f.removeCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout waiting for hold file %d to be removed", i)
		}
	}
}

func castPurgerMocksToTSMFiles(mocks []*mockPurgerTSMFile) []TSMFile {
	files := make([]TSMFile, len(mocks))
	for i, m := range mocks {
		files[i] = m
	}
	return files
}
