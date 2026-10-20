package compact_test

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/influxdata/influxdb/cmd/influx_tools/compact"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/stretchr/testify/require"
)

// TestCommand_CompactShard runs `influx_tools compact-shard` against a shard
// directory holding several TSM generations plus a tombstone, and checks that
// the output is a single TSM file that merges the inputs, honors overwrites
// and deletes, and replaces the original files.
func TestCommand_CompactShard(t *testing.T) {
	dir := t.TempDir()

	const (
		cpu  = "cpu,host=A#!~#value"
		mem  = "mem,host=A#!~#value"
		disk = "disk,host=A#!~#value"
	)

	gen1 := writeTSM(t, dir, 1, map[string][]tsm1.Value{
		cpu: {tsm1.NewValue(1, 1.0), tsm1.NewValue(2, 2.0)},
		mem: {tsm1.NewValue(1, int64(10))},
	})
	gen2 := writeTSM(t, dir, 2, map[string][]tsm1.Value{
		cpu: {tsm1.NewValue(2, 20.0), tsm1.NewValue(3, 3.0)}, // t=2 overwrites gen1
	})
	gen3 := writeTSM(t, dir, 3, map[string][]tsm1.Value{
		disk: {tsm1.NewValue(5, "full")},
	})

	// Delete mem entirely via a tombstone on the gen1 file.
	ts := tsm1.NewTombstoner(gen1, nil)
	require.NoError(t, ts.Add([][]byte{[]byte(mem)}))
	require.NoError(t, ts.Flush())
	tombstones, err := filepath.Glob(filepath.Join(dir, "*."+tsm1.TombstoneFileExtension))
	require.NoError(t, err)
	require.Len(t, tombstones, 1, "test setup should produce one tombstone file")

	var stdout, stderr bytes.Buffer
	cmd := compact.NewCommand()
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	require.NoError(t, cmd.Run([]string{"-path", dir, "-force"}), "stdout:\n%s", stdout.String())

	// Output generation is the max input generation; sequence is bumped.
	want := filepath.Join(dir, tsm1.DefaultFormatFileName(3, 2)+"."+tsm1.TSMFileExtension)
	require.Equal(t, []string{want}, listDir(t, dir), "shard should contain only the compacted file")
	require.Contains(t, stdout.String(), "Compaction succeeded")
	require.Contains(t, stdout.String(), want)

	for _, p := range append([]string{gen1, gen2, gen3}, tombstones...) {
		require.NoFileExists(t, p)
	}

	f, err := os.Open(want)
	require.NoError(t, err)
	r, err := tsm1.NewTSMReader(f)
	require.NoError(t, err)
	defer r.Close()

	require.False(t, r.HasTombstones(), "compacted file must not carry tombstones")
	require.Equal(t, 2, r.KeyCount(), "mem should be dropped by the tombstone")
	require.False(t, r.Contains([]byte(mem)))

	cpuVals, err := r.ReadAll([]byte(cpu))
	require.NoError(t, err)
	requireValues(t, []tsm1.Value{
		tsm1.NewValue(1, 1.0),
		tsm1.NewValue(2, 20.0),
		tsm1.NewValue(3, 3.0),
	}, cpuVals)

	diskVals, err := r.ReadAll([]byte(disk))
	require.NoError(t, err)
	requireValues(t, []tsm1.Value{tsm1.NewValue(5, "full")}, diskVals)
}

func writeTSM(t *testing.T, dir string, gen int, values map[string][]tsm1.Value) string {
	t.Helper()
	path := filepath.Join(dir, tsm1.DefaultFormatFileName(gen, 1)+"."+tsm1.TSMFileExtension)
	f, err := os.Create(path)
	require.NoError(t, err)

	w, err := tsm1.NewTSMWriter(f)
	require.NoError(t, err)

	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		require.NoError(t, w.Write([]byte(k), values[k]))
	}
	require.NoError(t, w.WriteIndex())
	require.NoError(t, w.Close())
	return path
}

func listDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, filepath.Join(dir, e.Name()))
	}
	return names
}

func requireValues(t *testing.T, exp, got []tsm1.Value) {
	t.Helper()
	require.Len(t, got, len(exp))
	for i := range exp {
		require.Equal(t, exp[i].UnixNano(), got[i].UnixNano(), "timestamp at index %d", i)
		require.Equal(t, exp[i].Value(), got[i].Value(), "value at index %d", i)
	}
}
