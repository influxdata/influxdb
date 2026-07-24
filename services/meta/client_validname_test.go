package meta_test

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hourSpec returns a minimal, valid retention policy spec with the given name.
func hourSpec(name string) *meta.RetentionPolicySpec {
	dur := time.Hour
	replicaN := 1
	return &meta.RetentionPolicySpec{
		Name:     name,
		Duration: &dur,
		ReplicaN: &replicaN,
	}
}

func TestMetaClient_CreateDatabase_RejectsInvalidName(t *testing.T) {
	t.Parallel()

	d, c := newClient()
	defer os.RemoveAll(d)
	defer c.Close()

	for _, name := range []string{"", "   ", "bad/name", `bad\name`, ".", ".."} {
		_, err := c.CreateDatabase(name)
		require.ErrorIsf(t, err, meta.ErrInvalidName, "CreateDatabase(%q)", name)
	}
	require.Empty(t, c.Databases(), "no database should have been created")
}

func TestMetaClient_CreateDatabase_TrimsWhitespace(t *testing.T) {
	t.Parallel()

	d, c := newClient()
	defer os.RemoveAll(d)
	defer c.Close()

	db, err := c.CreateDatabase("  db0  ")
	require.NoError(t, err)
	require.NotNil(t, db)
	assert.Equal(t, "db0", db.Name, "stored name should be trimmed")

	// The trimmed name is what is stored and matched; lookups do not trim.
	assert.NotNil(t, c.Database("db0"))
	assert.Nil(t, c.Database("  db0  "))

	// Re-creating with a differently-padded form resolves to the same database.
	db2, err := c.CreateDatabase(" db0 ")
	require.NoError(t, err)
	assert.Equal(t, "db0", db2.Name)
	require.Len(t, c.Databases(), 1, "padded name must not create a duplicate")
}

func TestMetaClient_CreateDatabaseWithRetentionPolicy_TrimsAndValidates(t *testing.T) {
	t.Parallel()

	d, c := newClient()
	defer os.RemoveAll(d)
	defer c.Close()

	db, err := c.CreateDatabaseWithRetentionPolicy("  db0  ", hourSpec("  rp0  "))
	require.NoError(t, err)
	require.NotNil(t, db)
	assert.Equal(t, "db0", db.Name, "database name should be trimmed")
	assert.Equal(t, "rp0", db.DefaultRetentionPolicy, "retention policy name should be trimmed")

	// Invalid database name is rejected and creates nothing new.
	_, err = c.CreateDatabaseWithRetentionPolicy("bad/db", hourSpec("rp0"))
	require.ErrorIs(t, err, meta.ErrInvalidName)

	// Invalid retention policy name is rejected before the database is created.
	_, err = c.CreateDatabaseWithRetentionPolicy("dbx", hourSpec("bad/rp"))
	require.ErrorIs(t, err, meta.ErrInvalidName)
	assert.Nil(t, c.Database("dbx"), "database must not be created when the RP name is invalid")
}

func TestMetaClient_CreateRetentionPolicy_TrimsAndValidates(t *testing.T) {
	t.Parallel()

	d, c := newClient()
	defer os.RemoveAll(d)
	defer c.Close()

	_, err := c.CreateDatabase("db0")
	require.NoError(t, err)

	// The RP name is trimmed and stored trimmed.
	rpi, err := c.CreateRetentionPolicy("db0", hourSpec("  rp0  "), false)
	require.NoError(t, err)
	require.NotNil(t, rpi)
	assert.Equal(t, "rp0", rpi.Name)

	got, err := c.RetentionPolicy("db0", "rp0")
	require.NoError(t, err)
	require.NotNil(t, got)

	// The database argument is also trimmed, so a padded reference resolves.
	rpi, err = c.CreateRetentionPolicy("  db0  ", hourSpec("rp1"), false)
	require.NoError(t, err)
	assert.Equal(t, "rp1", rpi.Name)

	// Invalid RP name is rejected.
	_, err = c.CreateRetentionPolicy("db0", hourSpec("bad/rp"), false)
	require.ErrorIs(t, err, meta.ErrInvalidName)

	// Invalid database name is rejected.
	_, err = c.CreateRetentionPolicy("bad/db", hourSpec("rp2"), false)
	require.ErrorIs(t, err, meta.ErrInvalidName)
}

// TestMetaClient_ConcurrentCreate hammers the create methods concurrently to
// stress the client's mutex for deadlocks and races (run with -race). All
// goroutines start together via an RWMutex so they contend maximally.
func TestMetaClient_ConcurrentCreate(t *testing.T) {
	t.Parallel()

	d, c := newClient()
	defer os.RemoveAll(d)
	defer c.Close()

	const numItems = 64

	// Build the padded names upfront so trimming is also exercised under contention.
	names := make([]string, numItems)
	for i := range numItems {
		names[i] = fmt.Sprintf("  db%03d  ", i)
	}

	var mu sync.RWMutex
	var concurrency, maxConcurrency atomic.Int64

	var wg sync.WaitGroup
	mu.Lock()
	for i := range numItems {
		wg.Add(1)
		go func(idx int) {
			mu.RLock()
			defer mu.RUnlock()
			defer wg.Done()

			cur := concurrency.Add(1)
			if old := maxConcurrency.Load(); cur > old {
				maxConcurrency.CompareAndSwap(old, cur)
			}

			if _, err := c.CreateDatabase(names[idx]); err != nil {
				t.Errorf("CreateDatabase(%q): %v", names[idx], err)
			}

			concurrency.Add(-1)
		}(i)
	}
	mu.Unlock() // release all goroutines simultaneously
	wg.Wait()
	t.Logf("max concurrency: %d", maxConcurrency.Load())

	// Every database must exist exactly once, stored under its trimmed name.
	for i := range numItems {
		name := fmt.Sprintf("db%03d", i)
		assert.NotNilf(t, c.Database(name), "database %q missing", name)
	}
	assert.Len(t, c.Databases(), numItems, "expected exactly %d databases", numItems)
}
