package write

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
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

// CsvToLineReader represents state of transformation from csv data to lien protocol reader
type CsvToLineReader struct {
	// csv reading
	csv *csv.Reader
	// Table collect information about used columns
	Table CsvTable
	// LineNumber represent line number of csv.Reader, 1-originated
	LineNumber int
	// when set, logs table data columns before reading data rows
	logTableDataColumns bool
	// flag that indicates whether data row was read
	dataRowAdded bool
	// log CSV data errors to sterr and continue with CSV processing
	skipRowOnError bool

	// reader results
	buffer     []byte
	lineBuffer []byte
	index      int
	finished   error
}

// LogTableColumns turns on/off logging of table data columns before reading data rows
func (state *CsvToLineReader) LogTableColumns(val bool) *CsvToLineReader {
	state.logTableDataColumns = val
	return state
}

// SkipRowOnError controls whether to fail on every CSV conversion error (false) or to log the error and continue (true)
func (state *CsvToLineReader) SkipRowOnError(val bool) *CsvToLineReader {
	state.skipRowOnError = val
	return state
}

func (state *CsvToLineReader) Read(p []byte) (n int, err error) {
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
		state.LineNumber++
		row, err := state.csv.Read()
		if err != nil {
			state.finished = err
			return state.Read(p)
		}
		state.csv.FieldsPerRecord = 0 // reset fields because every row can have different count of columns
		if state.LineNumber == 1 && len(row) == 1 && strings.HasPrefix(row[0], "sep=") && len(row[0]) > 4 {
			// separator specified in the first line
			state.csv.Comma = rune(row[0][4])
			continue
		}
		if state.Table.AddRow(row) {
			var err error
			state.lineBuffer = state.lineBuffer[:0] // reuse line buffer
			state.lineBuffer, err = state.Table.AppendLine(state.lineBuffer, row)
			if !state.dataRowAdded && state.logTableDataColumns {
				log.Println(state.Table.DataColumnsInfo())
			}
			state.dataRowAdded = true
			if err != nil {
				lineError := CsvLineError{state.LineNumber, err}
				if state.skipRowOnError {
					log.Println(lineError)
					continue
				}
				state.finished = lineError
				return state.Read(p)
			}

			state.buffer = append(state.buffer, state.lineBuffer...)
			state.buffer = append(state.buffer, '\n')
			break
		} else {
			state.dataRowAdded = false
		}
	}
	return state.Read(p)
}

// CsvToProtocolLines transforms csv data into line protocol data
func CsvToProtocolLines(reader io.Reader) *CsvToLineReader {
	return &CsvToLineReader{
		csv: csv.NewReader(reader),
	}
}
