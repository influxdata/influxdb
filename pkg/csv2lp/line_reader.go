package csv2lp

import (
	"io"
)

const (
	defaultBufSize = 4096
)

// LineReader wraps an io.Reader to count lines that go though read
// function and returns at most one line during every invovation of
// read. It provides a wrokaround to golang's CSV reader that
// does not expose current line number at all
// (see https://github.com/golang/go/issues/26679)
//
// At most one line is returned by every read in order to ensure that
// golang's CSV reader buffers at most one single line into its nested
// bufio.Reader, so that numbers reported are currect also for the
// CSV reader.
type LineReader struct {
	// LineNumber of the next read operation, 0 is the first line by default, can be set to 0 to count from 1
	LineNumber int
	// LastLineNumber is the number of the last read row
	LastLineNumber int

	// rs is a wrapped reader
	rd io.Reader // reader provided by the client
	// buf contains last data read from rd
	buf []byte
	// readPos is a read position in the buffer
	readPos int
	// bufSize is the length of cached data in the buffer
	bufSize int
	// err contain the last error during read
	err error
}

// NewLineReader returns a new LineReader that wraps an io.Reader
func NewLineReader(rd io.Reader) *LineReader {
	return NewLineReaderSize(rd, defaultBufSize)
}

// NewLineReaderSize returns a new Reader whose buffer has at least the specified
// size.
func NewLineReaderSize(rd io.Reader, size int) *LineReader {
	if size < 2 {
		size = 2
	}
	return &LineReader{
		rd:  rd,
		buf: make([]byte, size),
	}
}

// Read reads data into p. It fills in data that either does
// not contain \n or ends with \n.
// It returns the number of bytes read into p.
func (lr *LineReader) Read(p []byte) (n int, err error) {
	n = len(p)
	// handle patologic case of reading into empty array
	if n == 0 {
		if lr.readPos < lr.bufSize {
			return 0, nil
		}
		return 0, lr.readErr()
	}
	// read data into buf
	if lr.readPos == lr.bufSize {
		if lr.err != nil {
			return 0, lr.readErr()
		}
		lr.readPos = 0
		lr.bufSize = 0
		n, lr.err = lr.rd.Read(lr.buf)
		if n == 0 {
			return 0, lr.readErr()
		}
		lr.bufSize = n
	}
	// copy at most one line and don't overflow internal buffer or p
	i := 0
	for lr.readPos < lr.bufSize && i < n {
		lr.LastLineNumber = lr.LineNumber
		b := lr.buf[lr.readPos]
		lr.readPos++
		p[i] = b
		i++
		// read at most one line
		if b == '\n' {
			lr.LineNumber++
			break
		}
	}
	return i, nil
}

// readErr returns the last error
func (lr *LineReader) readErr() error {
	err := lr.err
	lr.err = nil
	return err
}
