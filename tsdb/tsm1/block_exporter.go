package tsm1

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/tsdb"
)

// BlockExporter writes all blocks in a file to a given format.
type BlockExporter interface {
	io.Closer
	ExportFile(filename string) error
}

// Ensure type implements interface.
var _ BlockExporter = (*SQLBlockExporter)(nil)

// SQLBlockExporter writes out all blocks for TSM files to SQL.
type SQLBlockExporter struct {
	w           io.Writer
	initialized bool // true when initial block written

	// Write schema, if true.
	ShowSchema bool
}

// NewSQLBlockExporter returns a new instance of SQLBlockExporter.
func NewSQLBlockExporter(w io.Writer) *SQLBlockExporter {
	return &SQLBlockExporter{
		w: w,

		ShowSchema: true,
	}
}

// Close ends the export and writes final output.
func (e *SQLBlockExporter) Close() error {
	return nil
}

// ExportFile writes all blocks of the TSM file.
func (e *SQLBlockExporter) ExportFile(filename string) error {
	if !e.initialized {
		if err := e.initialize(); err != nil {
			return err
		}
	}

	f, err := os.OpenFile(filename, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	r, err := NewTSMReader(f)
	if err != nil {
		return err
	}
	defer r.Close()

	itr := r.BlockIterator()
	if itr == nil {
		return errors.New("invalid TSM file, no block iterator")
	}

	fmt.Fprintln(e.w, `BEGIN TRANSACTION;`)
	for itr.Next() {
		key, minTime, maxTime, typ, checksum, buf, err := itr.Read()
		if err != nil {
			return err
		}

		// Extract organization & bucket ID.
		var record blockExportRecord
		record.Filename = filename
		if len(key) < 16 {
			record.Key = string(key)
		} else {
			record.OrgID, record.BucketID = tsdb.DecodeNameSlice(key[:16])
			record.Key = string(key[16:])
		}
		record.Type = typ
		record.MinTime = minTime
		record.MaxTime = maxTime
		record.Checksum = checksum
		record.Count = BlockCount(buf)

		if err := e.write(&record); err != nil {
			return err
		}
	}
	fmt.Fprintln(e.w, "COMMIT;")

	if err := r.Close(); err != nil {
		return fmt.Errorf("tsm1.SQLBlockExporter: cannot close reader: %s", err)
	}

	return nil
}

func (e *SQLBlockExporter) initialize() error {
	if e.ShowSchema {
		fmt.Fprintln(e.w, `
CREATE TABLE IF NOT EXISTS blocks (
	filename TEXT NOT NULL,
	org_id INTEGER NOT NULL,
	bucket_id INTEGER NOT NULL,
	key TEXT NOT NULL,
	"type" TEXT NOT NULL,
	min_time INTEGER NOT NULL,
	max_time INTEGER NOT NULL,
	checksum INTEGER NOT NULL,
	count INTEGER NOT NULL
);

CREATE INDEX idx_blocks_filename ON blocks (filename);
CREATE INDEX idx_blocks_org_id_bucket_id_key ON blocks (org_id, bucket_id, key);
`[1:])
	}

	e.initialized = true

	return nil
}

func (e *SQLBlockExporter) write(record *blockExportRecord) error {
	_, err := fmt.Fprintf(e.w,
		"INSERT INTO blocks (filename, org_id, bucket_id, key, type, min_time, max_time, checksum, count) VALUES (%s, %d, %d, %s, %s, %d, %d, %d, %d);\n",
		quoteSQL(record.Filename),
		record.OrgID,
		record.BucketID,
		quoteSQL(record.Key),
		quoteSQL(BlockTypeName(record.Type)),
		record.MinTime,
		record.MaxTime,
		record.Checksum,
		record.Count,
	)
	return err
}

type blockExportRecord struct {
	Filename string
	OrgID    influxdb.ID
	BucketID influxdb.ID
	Key      string
	Type     byte
	MinTime  int64
	MaxTime  int64
	Checksum uint32
	Count    int
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
