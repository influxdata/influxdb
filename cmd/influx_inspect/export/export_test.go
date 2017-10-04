package export

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/golang/snappy"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

type corpus map[string][]tsm1.Value

var (
	basicCorpus = corpus{
		tsm1.SeriesFieldKey("floats,k=f", "f"): []tsm1.Value{
			tsm1.NewValue(1, float64(1.5)),
			tsm1.NewValue(2, float64(3)),
		},
		tsm1.SeriesFieldKey("ints,k=i", "i"): []tsm1.Value{
			tsm1.NewValue(10, int64(15)),
			tsm1.NewValue(20, int64(30)),
		},
		tsm1.SeriesFieldKey("bools,k=b", "b"): []tsm1.Value{
			tsm1.NewValue(100, true),
			tsm1.NewValue(200, false),
		},
		tsm1.SeriesFieldKey("strings,k=s", "s"): []tsm1.Value{
			tsm1.NewValue(1000, "1k"),
			tsm1.NewValue(2000, "2k"),
		},
		tsm1.SeriesFieldKey("uints,k=u", "u"): []tsm1.Value{
			tsm1.NewValue(3000, uint64(45)),
			tsm1.NewValue(4000, uint64(60)),
		},
	}

	basicCorpusExpLines = []string{
		"floats,k=f f=1.5 1",
		"floats,k=f f=3 2",
		"ints,k=i i=15i 10",
		"ints,k=i i=30i 20",
		"bools,k=b b=true 100",
		"bools,k=b b=false 200",
		`strings,k=s s="1k" 1000`,
		`strings,k=s s="2k" 2000`,
		`uints,k=u u=45u 3000`,
		`uints,k=u u=60u 4000`,
	}

	escapeStringCorpus = corpus{
		tsm1.SeriesFieldKey("t", "s"): []tsm1.Value{
			tsm1.NewValue(1, `1. "quotes"`),
			tsm1.NewValue(2, `2. back\slash`),
			tsm1.NewValue(3, `3. bs\q"`),
		},
	}

	escCorpusExpLines = []string{
		`t s="1. \"quotes\"" 1`,
		`t s="2. back\\slash" 2`,
		`t s="3. bs\\q\"" 3`,
	}
)

func Test_exportWALFile(t *testing.T) {
	for _, c := range []struct {
		corpus corpus
		lines  []string
	}{
		{corpus: basicCorpus, lines: basicCorpusExpLines},
		{corpus: escapeStringCorpus, lines: escCorpusExpLines},
	} {
		walFile := writeCorpusToWALFile(c.corpus)
		defer os.Remove(walFile.Name())

		var out bytes.Buffer
		if err := newCommand().exportWALFile(walFile.Name(), &out, func() {}); err != nil {
			t.Fatal(err)
		}

		lines := strings.Split(out.String(), "\n")
		for _, exp := range c.lines {
			found := false
			for _, l := range lines {
				if exp == l {
					found = true
					break
				}
			}

			if !found {
				t.Fatalf("expected line %q to be in exported output:\n%s", exp, out.String())
			}
		}
	}

	// Missing .wal file should not cause a failure.
	var out bytes.Buffer
	if err := newCommand().exportWALFile("file-that-does-not-exist.wal", &out, func() {}); err != nil {
		t.Fatal(err)
	}
}

func Test_exportTSMFile(t *testing.T) {
	for _, c := range []struct {
		corpus corpus
		lines  []string
	}{
		{corpus: basicCorpus, lines: basicCorpusExpLines},
		{corpus: escapeStringCorpus, lines: escCorpusExpLines},
	} {
		tsmFile := writeCorpusToTSMFile(c.corpus)
		defer os.Remove(tsmFile.Name())

		var out bytes.Buffer
		if err := newCommand().exportTSMFile(tsmFile.Name(), &out); err != nil {
			t.Fatal(err)
		}

		lines := strings.Split(out.String(), "\n")
		for _, exp := range c.lines {
			found := false
			for _, l := range lines {
				if exp == l {
					found = true
					break
				}
			}

			if !found {
				t.Fatalf("expected line %q to be in exported output:\n%s", exp, out.String())
			}
		}
	}

	// Missing .tsm file should not cause a failure.
	var out bytes.Buffer
	if err := newCommand().exportTSMFile("file-that-does-not-exist.tsm", &out); err != nil {
		t.Fatal(err)
	}
}

var sink interface{}

func benchmarkExportTSM(c corpus, b *testing.B) {
	// Garbage collection is relatively likely to happen during export, so track allocations.
	b.ReportAllocs()

	f := writeCorpusToTSMFile(c)
	defer os.Remove(f.Name())

	cmd := newCommand()
	var out bytes.Buffer
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := cmd.exportTSMFile(f.Name(), &out); err != nil {
			b.Fatal(err)
		}

		sink = out.Bytes()
		out.Reset()
	}
}

func BenchmarkExportTSMFloats_100s_250vps(b *testing.B) {
	benchmarkExportTSM(makeFloatsCorpus(100, 250), b)
}

func BenchmarkExportTSMInts_100s_250vps(b *testing.B) {
	benchmarkExportTSM(makeIntsCorpus(100, 250), b)
}

func BenchmarkExportTSMBools_100s_250vps(b *testing.B) {
	benchmarkExportTSM(makeBoolsCorpus(100, 250), b)
}

func BenchmarkExportTSMStrings_100s_250vps(b *testing.B) {
	benchmarkExportTSM(makeStringsCorpus(100, 250), b)
}

func benchmarkExportWAL(c corpus, b *testing.B) {
	// Garbage collection is relatively likely to happen during export, so track allocations.
	b.ReportAllocs()

	f := writeCorpusToWALFile(c)
	defer os.Remove(f.Name())

	cmd := newCommand()
	var out bytes.Buffer
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := cmd.exportWALFile(f.Name(), &out, func() {}); err != nil {
			b.Fatal(err)
		}

		sink = out.Bytes()
		out.Reset()
	}
}

func BenchmarkExportWALFloats_100s_250vps(b *testing.B) {
	benchmarkExportWAL(makeFloatsCorpus(100, 250), b)
}

func BenchmarkExportWALInts_100s_250vps(b *testing.B) {
	benchmarkExportWAL(makeIntsCorpus(100, 250), b)
}

func BenchmarkExportWALBools_100s_250vps(b *testing.B) {
	benchmarkExportWAL(makeBoolsCorpus(100, 250), b)
}

func BenchmarkExportWALStrings_100s_250vps(b *testing.B) {
	benchmarkExportWAL(makeStringsCorpus(100, 250), b)
}

// newCommand returns a command that discards its output and that accepts all timestamps.
func newCommand() *Command {
	return &Command{
		Stderr:    ioutil.Discard,
		Stdout:    ioutil.Discard,
		startTime: math.MinInt64,
		endTime:   math.MaxInt64,
	}
}

// makeCorpus returns a new corpus filled with values generated by fn.
// The RNG passed to fn is seeded with numSeries * numValuesPerSeries, for predictable output.
func makeCorpus(numSeries, numValuesPerSeries int, fn func(*rand.Rand) interface{}) corpus {
	rng := rand.New(rand.NewSource(int64(numSeries) * int64(numValuesPerSeries)))
	var unixNano int64
	corpus := make(corpus, numSeries)
	for i := 0; i < numSeries; i++ {
		vals := make([]tsm1.Value, numValuesPerSeries)
		for j := 0; j < numValuesPerSeries; j++ {
			vals[j] = tsm1.NewValue(unixNano, fn(rng))
			unixNano++
		}

		k := fmt.Sprintf("m,t=%d", i)
		corpus[tsm1.SeriesFieldKey(k, "x")] = vals
	}

	return corpus
}

func makeFloatsCorpus(numSeries, numFloatsPerSeries int) corpus {
	return makeCorpus(numSeries, numFloatsPerSeries, func(rng *rand.Rand) interface{} {
		return rng.Float64()
	})
}

func makeIntsCorpus(numSeries, numIntsPerSeries int) corpus {
	return makeCorpus(numSeries, numIntsPerSeries, func(rng *rand.Rand) interface{} {
		// This will only return positive integers. That's probably okay.
		return rng.Int63()
	})
}

func makeBoolsCorpus(numSeries, numBoolsPerSeries int) corpus {
	return makeCorpus(numSeries, numBoolsPerSeries, func(rng *rand.Rand) interface{} {
		return rand.Int63n(2) == 1
	})
}

func makeStringsCorpus(numSeries, numStringsPerSeries int) corpus {
	return makeCorpus(numSeries, numStringsPerSeries, func(rng *rand.Rand) interface{} {
		// The string will randomly have 2-6 parts
		parts := make([]string, rand.Intn(4)+2)

		for i := range parts {
			// Each part is a random base36-encoded number
			parts[i] = strconv.FormatInt(rand.Int63(), 36)
		}

		// Join the individual parts with underscores.
		return strings.Join(parts, "_")
	})
}

// writeCorpusToWALFile writes the given corpus as a WAL file, and returns a handle to that file.
// It is the caller's responsibility to remove the returned temp file.
// writeCorpusToWALFile will panic on any error that occurs.
func writeCorpusToWALFile(c corpus) *os.File {
	walFile, err := ioutil.TempFile("", "export_test_corpus_wal")
	if err != nil {
		panic(err)
	}

	e := &tsm1.WriteWALEntry{Values: c}
	b, err := e.Encode(nil)
	if err != nil {
		panic(err)
	}

	w := tsm1.NewWALSegmentWriter(walFile)
	if err := w.Write(e.Type(), snappy.Encode(nil, b)); err != nil {
		panic(err)
	}

	if err := w.Flush(); err != nil {
		panic(err)
	}
	// (*tsm1.WALSegmentWriter).sync isn't exported, but it only Syncs the file anyway.
	if err := walFile.Sync(); err != nil {
		panic(err)
	}

	return walFile
}

// writeCorpusToTSMFile writes the given corpus as a TSM file, and returns a handle to that file.
// It is the caller's responsibility to remove the returned temp file.
// writeCorpusToTSMFile will panic on any error that occurs.
func writeCorpusToTSMFile(c corpus) *os.File {
	tsmFile, err := ioutil.TempFile("", "export_test_corpus_tsm")
	if err != nil {
		panic(err)
	}

	w, err := tsm1.NewTSMWriter(tsmFile)
	if err != nil {
		panic(err)
	}

	// Write the series in alphabetical order so that each test run is comparable,
	// given an identical corpus.
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := w.Write([]byte(k), c[k]); err != nil {
			panic(err)
		}
	}

	if err := w.WriteIndex(); err != nil {
		panic(err)
	}

	if err := w.Close(); err != nil {
		panic(err)
	}

	return tsmFile
}
