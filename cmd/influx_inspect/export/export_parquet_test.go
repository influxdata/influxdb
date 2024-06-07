package export

import (
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"io"
	"os"
	"testing"
)

var (
	myCorpus = corpus{
		tsm1.SeriesFieldKey("mym,tag=abc", "f"): []tsm1.Value{
			tsm1.NewValue(1, 1.5),
			tsm1.NewValue(2, 3.0),
		},
		tsm1.SeriesFieldKey("mym,tag=abc", "i"): []tsm1.Value{
			tsm1.NewValue(1, int64(15)),
			tsm1.NewValue(2, int64(30)),
		},
		tsm1.SeriesFieldKey("mym,tag=abc", "b"): []tsm1.Value{
			tsm1.NewValue(1, true),
			tsm1.NewValue(2, false),
		},
		tsm1.SeriesFieldKey("mym,tag=abc", "s"): []tsm1.Value{
			tsm1.NewValue(1, "1k"),
			tsm1.NewValue(2, "2k"),
		},
		tsm1.SeriesFieldKey("mym,tag=abc", "u"): []tsm1.Value{
			tsm1.NewValue(1, uint64(45)),
			tsm1.NewValue(2, uint64(60)),
		},
		tsm1.SeriesFieldKey("mym,tag=xyz", "f"): []tsm1.Value{
			tsm1.NewValue(1000, 1.5),
			tsm1.NewValue(2000, 3.0),
		},
		tsm1.SeriesFieldKey("mym,tag=xyz", "i"): []tsm1.Value{
			tsm1.NewValue(1000, int64(15)),
			tsm1.NewValue(2000, int64(30)),
		},
		tsm1.SeriesFieldKey("mym,tag=xyz", "b"): []tsm1.Value{
			tsm1.NewValue(1000, true),
			tsm1.NewValue(2000, false),
		},
		tsm1.SeriesFieldKey("mym,tag=xyz", "s"): []tsm1.Value{
			tsm1.NewValue(1000, "1k"),
			tsm1.NewValue(2000, "2k"),
		},
		tsm1.SeriesFieldKey("mym,tag=xyz", "u"): []tsm1.Value{
			tsm1.NewValue(1000, uint64(45)),
			tsm1.NewValue(2000, uint64(60)),
		},
		tsm1.SeriesFieldKey("alien,a=b", "level"): []tsm1.Value{
			tsm1.NewValue(1000000, "error"),
		},
	}
)

func TestExportParquet(t *testing.T) {
	for _, c := range []struct {
		corpus corpus
	}{
		{corpus: myCorpus},
	} {
		tsmFile := writeCorpusToTSMFile(c.corpus)
		defer os.Remove(tsmFile.Name())
		outdir := t.TempDir()

		cmd := newCommand()
		cmd.out = outdir
		cmd.writeValues = cmd.writeValuesParquet
		cmd.exportDone = cmd.exportDoneParquet
		cmd.measurement = "mym"
		cmd.parquet = true
		cmd.pqChunkSize = 100_000_000

		if err := cmd.writeTsmFiles(io.Discard, io.Discard, []string{tsmFile.Name()}); err != nil {
			t.Fatal(err)
		}
	}
}
