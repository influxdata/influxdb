package shard

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/influxdata/influxdb/cmd/influx_tools/internal/errlist"
	"github.com/influxdata/influxdb/pkg/data/gen"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

const (
	maxTSMFileSize = uint32(2048 * 1024 * 1024) // 2GB
)

type Writer struct {
	tw       tsm1.TSMWriter
	id       uint64
	path     string
	ext      string
	files    []string
	gen, seq int
	err      error
	buf      []byte
	auto     bool
}

type option func(w *Writer)

// Generation specifies the generation number of the tsm files.
func Generation(gen int) option {
	return func(w *Writer) {
		w.gen = gen
	}
}

// Sequence specifies the starting sequence number of the tsm files.
func Sequence(seq int) option {
	return func(w *Writer) {
		w.seq = seq
	}
}

// Temporary configures the writer to create tsm.tmp files.
func Temporary() option {
	return func(w *Writer) {
		w.ext = tsm1.TSMFileExtension + "." + tsm1.TmpTSMFileExtension
	}
}

// AutoNumber will read the existing TSM file names and use generation + 1
func AutoNumber() option {
	return func(w *Writer) {
		w.auto = true
	}
}

func NewWriter(id uint64, path string, opts ...option) *Writer {
	w := &Writer{id: id, path: path, gen: 1, seq: 1, ext: tsm1.TSMFileExtension}

	for _, opt := range opts {
		opt(w)
	}

	w.nextTSM()

	return w
}

func (w *Writer) Write(key []byte, values tsm1.Values) {
	if w.err != nil {
		return
	}

	if w.tw.Size() > maxTSMFileSize {
		w.closeTSM()
		w.nextTSM()
	}

	if err := w.tw.Write(key, values); err != nil {
		if err == tsm1.ErrMaxBlocksExceeded {
			w.closeTSM()
			w.nextTSM()
		} else {
			w.err = err
		}
	}
}

func (w *Writer) WriteV(key []byte, values gen.Values) {
	if w.err != nil {
		return
	}

	if w.tw.Size() > maxTSMFileSize {
		w.closeTSM()
		w.nextTSM()
	}

	minT, maxT := values.MinTime(), values.MaxTime()
	var err error
	if w.buf, err = values.Encode(w.buf); err != nil {
		w.err = err
		return
	}

	if err := w.tw.WriteBlock(key, minT, maxT, w.buf); err != nil {
		if err == tsm1.ErrMaxBlocksExceeded {
			w.closeTSM()
			w.nextTSM()
		} else {
			w.err = err
		}
	}
}

// Close closes the writer.
func (w *Writer) Close() {
	if w.tw != nil {
		w.closeTSM()
	}
}

// ShardID returns the shard number of the writer.
func (w *Writer) ShardID() uint64 { return w.id }

func (w *Writer) Err() error { return w.err }

// Files returns the full paths of all the files written by the Writer.
func (w *Writer) Files() []string { return w.files }

func (w *Writer) nextTSM() {
	fileName := filepath.Join(w.path, strconv.Itoa(int(w.id)), fmt.Sprintf("%09d-%09d.%s", w.gen, w.seq, w.ext))
	w.files = append(w.files, fileName)
	w.seq++

	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		w.err = err
		return
	}

	// Create the writer for the new TSM file.
	w.tw, err = tsm1.NewTSMWriter(fd)
	if err != nil {
		w.err = err
		return
	}
}

func (w *Writer) closeTSM() {
	el := errlist.NewErrorList()
	if err := w.tw.WriteIndex(); err != nil && err != tsm1.ErrNoValues {
		el.Add(err)
	}

	if err := w.tw.Close(); err != nil {
		el.Add(err)
	}

	err := el.Err()
	if err != nil {
		w.err = err
	}

	w.tw = nil
}
