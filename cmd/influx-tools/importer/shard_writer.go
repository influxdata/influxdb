package importer

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/influxdata/influxdb/cmd/influx-tools/internal/errlist"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

const (
	maxTSMFileSize = uint32(2048 * 1024 * 1024) // 2GB
)

type shardWriter struct {
	w        tsm1.TSMWriter
	id       uint64
	path     string
	gen, seq int
	err      error
}

func newShardWriter(id uint64, path string) *shardWriter {
	t := &shardWriter{id: id, path: path, gen: 1, seq: 1}
	return t
}

func (t *shardWriter) Write(key []byte, values tsm1.Values) {
	if t.err != nil {
		return
	}

	if t.w == nil {
		t.nextTSM()
	}

	if t.w.Size() > maxTSMFileSize {
		t.closeTSM()
		t.nextTSM()
	}

	if err := t.w.Write(key, values); err != nil {
		if err == tsm1.ErrMaxBlocksExceeded {
			t.closeTSM()
			t.nextTSM()
		} else {
			t.err = err
		}
	}
}

func (t *shardWriter) Close() {
	if t.w != nil {
		t.closeTSM()
	}
}

func (t *shardWriter) Err() error { return t.err }

func (t *shardWriter) nextTSM() {
	fileName := filepath.Join(t.path, strconv.Itoa(int(t.id)), fmt.Sprintf("%09d-%09d.%s", t.gen, t.seq, tsm1.TSMFileExtension))
	t.seq++

	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.err = err
		return
	}

	// Create the writer for the new TSM file.
	t.w, err = tsm1.NewTSMWriter(fd)
	if err != nil {
		t.err = err
		return
	}
}

func (t *shardWriter) closeTSM() {
	el := errlist.NewErrorList()
	if err := t.w.WriteIndex(); err != nil && err != tsm1.ErrNoValues {
		el.Add(err)
	}

	if err := t.w.Close(); err != nil {
		el.Add(err)
	}

	err := el.Err()
	if err != nil {
		t.err = err
	}

	t.w = nil
}
