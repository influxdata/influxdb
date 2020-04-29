package csv2lp

import (
	"io"
)

// skipFirstLines is an io.Reader that skips first lines
type skipFirstLines struct {
	reader    io.Reader
	skipLines int
	// line is a mutable variable that increases until skipLines is reached
	line int
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
			if p[i] == '\n' {
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
	return state.reader.Read(p)
}

// SkipHeaderLinesReader wraps a reader to skip the first skipLines lines
func SkipHeaderLinesReader(skipLines int, reader io.Reader) io.Reader {
	return &skipFirstLines{
		skipLines: skipLines,
		reader:    reader,
	}
}
