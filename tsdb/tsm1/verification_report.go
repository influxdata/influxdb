package tsm1

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/tsdb"
)

// VerificationReport represents a report for verifying TSM1 file consistency.
type VerificationReport struct {
	Stderr io.Writer
	Stdout io.Writer

	Paths    []string     // Files to verify.
	OrgID    *influxdb.ID // Optional organization filter.
	BucketID *influxdb.ID // Optional bucket filter.
}

// NewVerificationReport returns a new instance of VerificationReport.
func NewVerificationReport() *VerificationReport {
	return &VerificationReport{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run executes the report.
func (r *VerificationReport) Run() (*VerificationReportResult, error) {
	var result VerificationReportResult
	start := time.Now()

	// Process each file.
	for _, path := range r.Paths {
		file, err := r.VerifyFile(path)
		if err != nil {
			return nil, err
		}

		result.TotalBlockN += file.TotalBlockN
		result.CorruptBlockN += file.CorruptBlockN
		result.Files = append(result.Files, file)
	}

	// Calculate elapsed time.
	result.Elapsed = time.Since(start)

	return &result, nil
}

// VerifyFile reads and verifies a single TSM file.
func (r *VerificationReport) VerifyFile(filename string) (*VerificationReportFileResult, error) {
	var result VerificationReportFileResult
	result.Name = filename

	f, err := os.OpenFile(filename, os.O_RDONLY, 0600)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	rd, err := NewTSMReader(f)
	if err != nil {
		return nil, err
	}
	defer rd.Close()

	// Iterate over every block and verify that it's readable and the checksum matches.
	for itr, n := rd.BlockIterator(), 0; itr.Next(); n++ {
		key, _, _, _, checksum, buf, err := itr.Read()

		// Filter by org/bucket ID, if specified.
		if key != nil {
			if orgID, bucketID := tsdb.DecodeNameSlice(key[:16]); r.OrgID != nil && *r.OrgID != orgID {
				continue
			} else if r.BucketID != nil && *r.BucketID != bucketID {
				continue
			}
		}

		if err != nil {
			result.CorruptBlockN++
			fmt.Fprintf(r.Stdout, "%s: could not get checksum for key 0x%x block %d due to error: %q\n", filename, key, n, err)
		} else if expected := crc32.ChecksumIEEE(buf); checksum != expected {
			result.CorruptBlockN++
			fmt.Fprintf(r.Stdout, "%s: got 0x%x but expected 0x%x for key 0x%x, block %d\n", filename, checksum, expected, key, n)
		}

		result.TotalBlockN++
	}

	// 	Report file as healthy if no corrupt blocks exist.
	if result.CorruptBlockN == 0 {
		fmt.Fprintf(r.Stdout, "%s: healthy\n", filename)
	}

	// Verify reader & file can close without error.
	if err := rd.Close(); err != nil {
		return nil, err
	}

	return &result, nil
}

// VerificationReportResult represents the results of VerificationReport.
type VerificationReportResult struct {
	TotalBlockN   int           // total number of blocks
	CorruptBlockN int           // number of corrupt blocks
	Elapsed       time.Duration // time to verify

	Files []*VerificationReportFileResult // individual file results
}

// VerificationReportFileResult represents the results of a single file verification.
type VerificationReportFileResult struct {
	Name          string
	TotalBlockN   int // total number of blocks
	CorruptBlockN int // number of corrupt blocks
}
