package shard

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb/v2/pkg/data/gen"
	"github.com/influxdata/influxdb/v2/tsdb/tsm1"
)

const (
	maxTSMFileSize = uint32(2048 * 1024 * 1024) // 2GB
)

type Writer struct {
	tw       tsm1.TSMWriter
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

func NewWriter(path string, opts ...option) (*Writer, error) {
	w := &Writer{path: path, gen: 1, seq: 1, ext: tsm1.TSMFileExtension}

	for _, opt := range opts {
		opt(w)
	}

	if w.auto {
		err := w.readExisting()
		if err != nil {
			return nil, err
		}
	}

	w.nextTSM()
	if w.err != nil {
		return nil, w.err
	}

	return w, nil
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

func (w *Writer) Err() error { return w.err }

// Files returns the full paths of all the files written by the Writer.
func (w *Writer) Files() []string { return w.files }

func (w *Writer) nextTSM() {
	fileName := filepath.Join(w.path, fmt.Sprintf("%s.%s", tsm1.DefaultFormatFileName(w.gen, w.seq), w.ext))
	w.files = append(w.files, fileName)
	w.seq++

	if fi, _ := os.Stat(fileName); fi != nil {
		w.err = fmt.Errorf("attempted to overwrite an existing TSM file: %q", fileName)
		return
	}

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
	if err := w.tw.WriteIndex(); err != nil && err != tsm1.ErrNoValues {
		w.err = err
	}

	if err := w.tw.Close(); err != nil && w.err == nil {
		w.err = err
	}

	w.tw = nil
}

func (w *Writer) readExisting() error {
	files, err := filepath.Glob(filepath.Join(w.path, fmt.Sprintf("*.%s", tsm1.TSMFileExtension)))
	if err != nil {
		return err
	}

	for _, f := range files {
		generation, _, err := tsm1.DefaultParseFileName(f)
		if err != nil {
			return err
		}

		if generation >= w.gen {
			w.gen = generation + 1
		}
	}
	return nil
}
