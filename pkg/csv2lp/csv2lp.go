// Package csv2lp transforms CSV data to InfluxDB line protocol
package csv2lp

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"strings"
)

// CsvLineError is returned for csv conversion errors
type CsvLineError struct {
	// 1 is the first line
	Line int
	Err  error
}

func (e CsvLineError) Error() string {
	if e.Line > 0 {
		return fmt.Sprintf("line %d: %v", e.Line, e.Err)
	}
	return fmt.Sprintf("%v", e.Err)
}

// CreateRowColumnError wraps an existing error to add line and column coordinates
func CreateRowColumnError(line int, columnLabel string, err error) CsvLineError {
	return CsvLineError{
		Line: line,
		Err: CsvColumnError{
			Column: columnLabel,
			Err:    err,
		},
	}
}

// CsvToLineReader represents state of transformation from csv data to lien protocol reader
type CsvToLineReader struct {
	// csv reading
	csv *csv.Reader
	// Table collects information about used columns
	Table CsvTable
	// LineNumber represents line number of csv.Reader, 1 is the first
	LineNumber int
	// when true, log table data columns before reading data rows
	logTableDataColumns bool
	// state variable that indicates whether any data row was read
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

// Read implements io.Reader that returns protocol lines
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
		if parseError, ok := err.(*csv.ParseError); ok && parseError.Err == csv.ErrFieldCount {
			// every row can have different number of columns
			err = nil
		}

		if err != nil {
			state.finished = err
			return state.Read(p)
		}
		if state.LineNumber == 1 && len(row) == 1 && strings.HasPrefix(row[0], "sep=") && len(row[0]) > 4 {
			// separator specified in the first line
			state.csv.Comma = rune(row[0][4])
			continue
		}
		if state.Table.AddRow(row) {
			var err error
			state.lineBuffer = state.lineBuffer[:0] // reuse line buffer
			state.lineBuffer, err = state.Table.AppendLine(state.lineBuffer, row, state.LineNumber)
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

// CsvToLineProtocol transforms csv data into line protocol data
func CsvToLineProtocol(reader io.Reader) *CsvToLineReader {
	csv := csv.NewReader(reader)
	csv.ReuseRecord = true
	return &CsvToLineReader{
		csv: csv,
	}
}
