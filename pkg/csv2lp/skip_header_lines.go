package csv2lp

import (
	"io"
)

// skipFirstLines is an io.Reader that skips first lines
type skipFirstLines struct {
	// reader provides data
	reader io.Reader
	// skipLines contains the lines to skip
	skipLines int
	// line is a mutable variable that increases until skipLines is reached
	line int
	// quotedString indicates whether a quoted CSV string is being read,
	// a new line inside a quoted string does not start a new CSV line
	quotedString bool
}

// Read implements io.Reader
func (state *skipFirstLines) Read(p []byte) (n int, err error) {
skipHeaderLines:
	for state.line < state.skipLines {
		n, err := state.reader.Read(p)
		if n == 0 {
			return n, err
		}
		for i := 0; i < n; i++ {
			switch p[i] {
			case '"':
				// a quoted string starts or stops
				state.quotedString = !state.quotedString
			case '\n':
				if !state.quotedString {
					state.line++
					if state.line == state.skipLines {
						// modify the buffer and return
						if i == n-1 {
							if err != nil {
								return 0, err
							}
							// continue with the next chunk
							break skipHeaderLines
						} else {
							// copy all bytes after the newline
							for j := i + 1; j < n; j++ {
								p[j-i-1] = p[j]
							}
							return n - i - 1, err
						}
					}
				}
			}
		}
	}
	return state.reader.Read(p)
}

// SkipHeaderLinesReader wraps a reader to skip the first skipLines lines in CSV data input
func SkipHeaderLinesReader(skipLines int, reader io.Reader) io.Reader {
	return &skipFirstLines{
		skipLines: skipLines,
		reader:    reader,
	}
}
