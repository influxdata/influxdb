package tsi1

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"go.uber.org/zap"
)

// SQLIndexExporter writes out all TSI data for an index to a SQL export.
type SQLIndexExporter struct {
	w io.Writer

	initialized bool

	// Logs non-fatal warnings.
	Logger *zap.Logger

	// Write schema, if true.
	ShowSchema bool
}

// NewSQLIndexExporter returns a new instance of SQLIndexExporter.
func NewSQLIndexExporter(w io.Writer) *SQLIndexExporter {
	return &SQLIndexExporter{
		w: w,

		Logger:     zap.NewNop(),
		ShowSchema: true,
	}
}

// Close ends the export and writes final output.
func (e *SQLIndexExporter) Close() error {
	return nil
}

// ExportIndex writes all blocks of the TSM file.
func (e *SQLIndexExporter) ExportIndex(idx *Index) error {
	if err := e.initialize(); err != nil {
		return err
	}

	fmt.Fprintln(e.w, `BEGIN TRANSACTION;`)

	// Iterate over each measurement across all partitions.
	itr, err := idx.MeasurementIterator()
	if err != nil {
		return err
	} else if itr == nil {
		return nil
	}
	defer itr.Close()

	for {
		name, err := itr.Next()
		if err != nil {
			return err
		} else if name == nil {
			break
		}

		if err := e.exportMeasurement(idx, name); err != nil {
			return err
		}
	}

	fmt.Fprintln(e.w, "COMMIT;")
	return nil
}

func (e *SQLIndexExporter) exportMeasurement(idx *Index, name []byte) error {
	if err := e.exportMeasurementSeries(idx, name); err != nil {
		return err
	}

	itr, err := idx.TagKeyIterator(name)
	if err != nil {
		return err
	} else if itr == nil {
		return nil
	}
	defer itr.Close()

	for {
		key, err := itr.Next()
		if err != nil {
			return err
		} else if key == nil {
			break
		}

		if err := e.exportTagKey(idx, name, key); err != nil {
			return err
		}
	}
	return nil
}

func (e *SQLIndexExporter) exportMeasurementSeries(idx *Index, name []byte) error {
	itr, err := idx.MeasurementSeriesIDIterator(name)
	if err != nil {
		return err
	} else if itr == nil {
		return nil
	}
	defer itr.Close()

	for {
		elem, err := itr.Next()
		if err != nil {
			return err
		} else if elem.SeriesID == 0 {
			break
		}

		if _, err := fmt.Fprintf(e.w, "INSERT INTO measurement_series (name, series_id) VALUES (%s, %d);\n",
			quoteSQL(string(name)),
			elem.SeriesID); err != nil {
			return err
		}
	}
	return nil
}

func (e *SQLIndexExporter) exportTagKey(idx *Index, name, key []byte) error {
	itr, err := idx.TagValueIterator(name, key)
	if err != nil {
		return err
	} else if itr == nil {
		return nil
	}
	defer itr.Close()

	for {
		value, err := itr.Next()
		if err != nil {
			return err
		} else if value == nil {
			break
		}

		if err := e.exportTagValue(idx, name, key, value); err != nil {
			return err
		}
	}
	return nil
}

func (e *SQLIndexExporter) exportTagValue(idx *Index, name, key, value []byte) error {
	itr, err := idx.TagValueSeriesIDIterator(name, key, value)
	if err != nil {
		return err
	} else if itr == nil {
		return nil
	}
	defer itr.Close()

	for {
		elem, err := itr.Next()
		if err != nil {
			return err
		} else if elem.SeriesID == 0 {
			break
		}

		// Replace special case keys for measurement & field.
		if bytes.Equal(key, []byte{0}) {
			key = []byte("_measurement")
		} else if bytes.Equal(key, []byte{0xff}) {
			key = []byte("_field")
		}

		if _, err := fmt.Fprintf(e.w,
			"INSERT INTO tag_value_series (name, key, value, series_id) VALUES (%s, %s, %s, %d);\n",
			quoteSQL(string(name)),
			quoteSQL(string(key)),
			quoteSQL(string(value)),
			elem.SeriesID,
		); err != nil {
			return err
		}
	}
	return nil
}

func (e *SQLIndexExporter) initialize() error {
	if e.initialized {
		return nil
	}
	e.initialized = true

	if !e.ShowSchema {
		return nil
	}
	fmt.Fprintln(e.w, `
CREATE TABLE IF NOT EXISTS measurement_series (
	name      TEXT NOT NULL,
	series_id INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tag_value_series (
	name      TEXT NOT NULL,
	key       TEXT NOT NULL,
	value     TEXT NOT NULL,
	series_id INTEGER NOT NULL
);
`[1:])

	return nil
}

func quoteSQL(s string) string {
	return `'` + sqlReplacer.Replace(toValidUTF8(s)) + `'`
}

var sqlReplacer = strings.NewReplacer(`'`, `''`, "\x00", "")

func toValidUTF8(s string) string {
	return strings.Map(func(r rune) rune {
		if r == utf8.RuneError {
			return -1
		}
		return r
	}, s)
}
