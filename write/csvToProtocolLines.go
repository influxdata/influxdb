package write

import (
	"encoding/csv"
	"fmt"
	"io"
	"strings"
)

// CsvLineError is returned for csv conversion errors
// Line numbers are 1-indexed
type CsvLineError struct {
	Line int
	Err  error
}

func (e CsvLineError) Error() string {
	return fmt.Sprintf("line %d: %v", e.Line, e.Err)
}

type lineReader struct {
	// csv reading
	csv        *csv.Reader
	table      CsvTable
	lineNumber int

	// reader results
	buffer   []byte
	index    int
	finished error
}

func (state *lineReader) Read(p []byte) (n int, err error) {
	// state1: finished
	if state.finished != nil {
		return 0, state.finished
	}
	// state2: some data are in the buffer to copy
	if len(state.buffer) > state.index {
		// we have remaining bytes to copy
		if len(state.buffer)-state.index > len(p) {
			// copy a part of the buffer
			copy(p, state.buffer[state.index:state.index+len(p)])
			state.index += len(p)
			return len(p), nil
		}
		// copy the entire buffer
		n = len(state.buffer) - state.index
		copy(p[:n], state.buffer[state.index:])
		state.buffer = state.buffer[:0]
		state.index = 0
		return n, nil
	}
	// state3: fill buffer with data to read from
	for {
		// Read each record from csv
		state.lineNumber++
		row, err := state.csv.Read()
		if err != nil {
			state.finished = err
			return state.Read(p)
		}
		state.csv.FieldsPerRecord = 0 // reset fields because every row can have different count of columns
		if state.lineNumber == 1 && len(row) == 1 && strings.HasPrefix(row[0], "sep=") && len(row[0]) > 4 {
			// separator specified in the first line
			state.csv.Comma = rune(row[0][4])
			continue
		}
		if state.table.AddRow(row) {
			buffer, err := state.table.AppendLine(state.buffer, row)
			if err != nil {
				state.finished = CsvLineError{state.lineNumber, err}
				return state.Read(p)
			}
			state.buffer = append(buffer, '\n')
			break
		}
	}
	return state.Read(p)
}

// CsvToProtocolLines transforms csv data into line protocol data
func CsvToProtocolLines(reader io.Reader) io.Reader {
	return &lineReader{
		csv: csv.NewReader(reader),
	}
}
