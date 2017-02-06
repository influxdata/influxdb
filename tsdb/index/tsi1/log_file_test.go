package tsi1_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

// Ensure log file can append series.
func TestLogFile_AddSeries(t *testing.T) {
	f := MustOpenLogFile()
	defer f.Close()

	// Add test data.
	if err := f.AddSeries([]byte("mem"), models.Tags{{Key: []byte("host"), Value: []byte("serverA")}}); err != nil {
		t.Fatal(err)
	} else if err := f.AddSeries([]byte("cpu"), models.Tags{{Key: []byte("region"), Value: []byte("us-east")}}); err != nil {
		t.Fatal(err)
	} else if err := f.AddSeries([]byte("cpu"), models.Tags{{Key: []byte("region"), Value: []byte("us-west")}}); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	itr := f.MeasurementIterator()
	if e := itr.Next(); e == nil || string(e.Name()) != "cpu" {
		t.Fatalf("unexpected measurement: %#v", e)
	} else if e := itr.Next(); e == nil || string(e.Name()) != "mem" {
		t.Fatalf("unexpected measurement: %#v", e)
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected eof, got: %#v", e)
	}

	// Reopen file and re-verify.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	itr = f.MeasurementIterator()
	if e := itr.Next(); e == nil || string(e.Name()) != "cpu" {
		t.Fatalf("unexpected measurement: %#v", e)
	} else if e := itr.Next(); e == nil || string(e.Name()) != "mem" {
		t.Fatalf("unexpected measurement: %#v", e)
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected eof, got: %#v", e)
	}
}

// Ensure log file can delete an existing measurement.
func TestLogFile_DeleteMeasurement(t *testing.T) {
	f := MustOpenLogFile()
	defer f.Close()

	// Add test data.
	if err := f.AddSeries([]byte("mem"), models.Tags{{Key: []byte("host"), Value: []byte("serverA")}}); err != nil {
		t.Fatal(err)
	} else if err := f.AddSeries([]byte("cpu"), models.Tags{{Key: []byte("region"), Value: []byte("us-east")}}); err != nil {
		t.Fatal(err)
	} else if err := f.AddSeries([]byte("cpu"), models.Tags{{Key: []byte("region"), Value: []byte("us-west")}}); err != nil {
		t.Fatal(err)
	}

	// Remove measurement.
	if err := f.DeleteMeasurement([]byte("cpu")); err != nil {
		t.Fatal(err)
	}

	// Verify data.
	itr := f.MeasurementIterator()
	if e := itr.Next(); string(e.Name()) != "cpu" || !e.Deleted() {
		t.Fatalf("unexpected measurement: %s/%v", e.Name(), e.Deleted())
	} else if e := itr.Next(); string(e.Name()) != "mem" || e.Deleted() {
		t.Fatalf("unexpected measurement: %s/%v", e.Name(), e.Deleted())
	} else if e := itr.Next(); e != nil {
		t.Fatalf("expected eof, got: %#v", e)
	}
}

// LogFile is a test wrapper for tsi1.LogFile.
type LogFile struct {
	*tsi1.LogFile
}

// NewLogFile returns a new instance of LogFile with a temporary file path.
func NewLogFile() *LogFile {
	file, err := ioutil.TempFile("", "tsi1-log-file-")
	if err != nil {
		panic(err)
	}
	file.Close()

	return &LogFile{LogFile: tsi1.NewLogFile(file.Name())}
}

// MustOpenLogFile returns a new, open instance of LogFile. Panic on error.
func MustOpenLogFile() *LogFile {
	f := NewLogFile()
	if err := f.Open(); err != nil {
		panic(err)
	}
	return f
}

// Close closes the log file and removes it from disk.
func (f *LogFile) Close() error {
	defer os.Remove(f.Path())
	return f.LogFile.Close()
}

// Reopen closes and reopens the file.
func (f *LogFile) Reopen() error {
	if err := f.LogFile.Close(); err != nil {
		return err
	}
	if err := f.LogFile.Open(); err != nil {
		return err
	}
	return nil
}

// CreateLogFile creates a new temporary log file and adds a list of series.
func CreateLogFile(series []Series) (*LogFile, error) {
	f := MustOpenLogFile()
	for _, serie := range series {
		if err := f.AddSeries(serie.Name, serie.Tags); err != nil {
			return nil, err
		}
	}
	return f, nil
}

// GenerateLogFile generates a log file from a set of series based on the count arguments.
// Total series returned will equal measurementN * tagN * valueN.
func GenerateLogFile(measurementN, tagN, valueN int) (*LogFile, error) {
	tagValueN := pow(valueN, tagN)

	f := MustOpenLogFile()
	for i := 0; i < measurementN; i++ {
		name := []byte(fmt.Sprintf("measurement%d", i))

		// Generate tag sets.
		for j := 0; j < tagValueN; j++ {
			var tags models.Tags
			for k := 0; k < tagN; k++ {
				key := []byte(fmt.Sprintf("key%d", k))
				value := []byte(fmt.Sprintf("value%d", (j / pow(valueN, k) % valueN)))
				tags = append(tags, models.Tag{Key: key, Value: value})
			}
			if err := f.AddSeries(name, tags); err != nil {
				return nil, err
			}
		}
	}
	return f, nil
}

func MustGenerateLogFile(measurementN, tagN, valueN int) *LogFile {
	f, err := GenerateLogFile(measurementN, tagN, valueN)
	if err != nil {
		panic(err)
	}
	return f
}
