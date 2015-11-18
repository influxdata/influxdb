package tsm1_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/influxdb/influxdb/models"
	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

func TestWALWriter_WritePoints_Single(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := tsm1.NewWALSegmentWriter(f)

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")

	points := []models.Point{
		p1,
	}

	entry := &tsm1.WriteWALEntry{
		Points: points,
	}

	if err := w.Write(entry); err != nil {
		fatal(t, "write points", err)
	}

	if _, err := f.Seek(0, os.SEEK_SET); err != nil {
		fatal(t, "seek", err)
	}

	r := tsm1.NewWALSegmentReader(f)

	if !r.Next() {
		t.Fatalf("expected next, got false")
	}

	we, err := r.Read()
	if err != nil {
		fatal(t, "read entry", err)
	}

	e, ok := we.(*tsm1.WriteWALEntry)
	if !ok {
		t.Fatalf("expected WriteWALEntry: got %#v", e)
	}

	for i, p := range e.Points {
		if exp, got := points[i].String(), p.String(); exp != got {
			t.Fatalf("points mismatch: got %v, exp %v", got, exp)
		}
	}
}

func TestWALWriter_WritePoints_Multiple(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := tsm1.NewWALSegmentWriter(f)

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")
	p2 := parsePoint("cpu,host=B value=1.1 1000000000")

	exp := [][]models.Point{
		[]models.Point{
			p1,
		},
		[]models.Point{
			p2,
		},
	}

	for _, e := range exp {
		entry := &tsm1.WriteWALEntry{
			Points: e,
		}

		if err := w.Write(entry); err != nil {
			fatal(t, "write points", err)
		}
	}

	// Seek back to the beinning of the file for reading
	if _, err := f.Seek(0, os.SEEK_SET); err != nil {
		fatal(t, "seek", err)
	}

	r := tsm1.NewWALSegmentReader(f)

	for _, ep := range exp {
		if !r.Next() {
			t.Fatalf("expected next, got false")
		}

		we, err := r.Read()
		if err != nil {
			fatal(t, "read entry", err)
		}

		e, ok := we.(*tsm1.WriteWALEntry)
		if !ok {
			t.Fatalf("expected WriteWALEntry: got %#v", e)
		}

		points := e.Points
		for i, p := range ep {
			if exp, got := points[i].String(), p.String(); exp != got {
				t.Fatalf("points mismatch: got %v, exp %v", got, exp)
			}
		}
	}
}

func TestWALWriter_WriteDelete_Single(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := tsm1.NewWALSegmentWriter(f)

	entry := &tsm1.DeleteWALEntry{
		Keys: []string{"cpu"},
	}

	if err := w.Write(entry); err != nil {
		fatal(t, "write points", err)
	}

	if _, err := f.Seek(0, os.SEEK_SET); err != nil {
		fatal(t, "seek", err)
	}

	r := tsm1.NewWALSegmentReader(f)

	if !r.Next() {
		t.Fatalf("expected next, got false")
	}

	we, err := r.Read()
	if err != nil {
		fatal(t, "read entry", err)
	}

	e, ok := we.(*tsm1.DeleteWALEntry)
	if !ok {
		t.Fatalf("expected WriteWALEntry: got %#v", e)
	}

	if got, exp := len(e.Keys), len(entry.Keys); got != exp {
		t.Fatalf("key length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := e.Keys[0], entry.Keys[0]; got != exp {
		t.Fatalf("key mismatch: got %v, exp %v", got, exp)
	}
}

func TestWALWriter_WritePointsDelete_Multiple(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := tsm1.NewWALSegmentWriter(f)

	p1 := parsePoint("cpu,host=A value=1.1 1000000000")

	write := &tsm1.WriteWALEntry{
		Points: []models.Point{p1},
	}

	if err := w.Write(write); err != nil {
		fatal(t, "write points", err)
	}

	// Write the delete entry
	deleteEntry := &tsm1.DeleteWALEntry{
		Keys: []string{"cpu"},
	}

	if err := w.Write(deleteEntry); err != nil {
		fatal(t, "write points", err)
	}

	// Seek back to the beinning of the file for reading
	if _, err := f.Seek(0, os.SEEK_SET); err != nil {
		fatal(t, "seek", err)
	}

	r := tsm1.NewWALSegmentReader(f)

	// Read the write points first
	if !r.Next() {
		t.Fatalf("expected next, got false")
	}

	we, err := r.Read()
	if err != nil {
		fatal(t, "read entry", err)
	}

	e, ok := we.(*tsm1.WriteWALEntry)
	if !ok {
		t.Fatalf("expected WriteWALEntry: got %#v", e)
	}

	points := e.Points
	for i, p := range write.Points {
		if exp, got := points[i].String(), p.String(); exp != got {
			t.Fatalf("points mismatch: got %v, exp %v", got, exp)
		}
	}

	// Read the delete second
	if !r.Next() {
		t.Fatalf("expected next, got false")
	}

	we, err = r.Read()
	if err != nil {
		fatal(t, "read entry", err)
	}

	de, ok := we.(*tsm1.DeleteWALEntry)
	if !ok {
		t.Fatalf("expected DeleteWALEntry: got %#v", e)
	}

	if got, exp := len(de.Keys), len(deleteEntry.Keys); got != exp {
		t.Fatalf("key length mismatch: got %v, exp %v", got, exp)
	}

	if got, exp := de.Keys[0], deleteEntry.Keys[0]; got != exp {
		t.Fatalf("key mismatch: got %v, exp %v", got, exp)
	}
}

func TestWAL_ClosedSegments(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	w := tsm1.NewWAL(dir)
	if err := w.Open(); err != nil {
		t.Fatalf("error opening WAL: %v", err)
	}

	files, err := w.ClosedSegments()
	if err != nil {
		t.Fatalf("error getting closed segments: %v", err)
	}

	if got, exp := len(files), 0; got != exp {
		t.Fatalf("close segment length mismatch: got %v, exp %v", got, exp)
	}

	if err := w.WritePoints([]models.Point{
		parsePoint("cpu,host=A value=1.1 1000000000"),
	}); err != nil {
		t.Fatalf("error writing points: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("error closing wal: %v", err)
	}

	// Re-open the WAL
	w = tsm1.NewWAL(dir)
	defer w.Close()
	if err := w.Open(); err != nil {
		t.Fatalf("error opening WAL: %v", err)
	}

	files, err = w.ClosedSegments()
	if err != nil {
		t.Fatalf("error getting closed segments: %v", err)
	}
	if got, exp := len(files), 1; got != exp {
		t.Fatalf("close segment length mismatch: got %v, exp %v", got, exp)
	}
}

func TestWAL_Delete(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)

	w := tsm1.NewWAL(dir)
	if err := w.Open(); err != nil {
		t.Fatalf("error opening WAL: %v", err)
	}

	files, err := w.ClosedSegments()
	if err != nil {
		t.Fatalf("error getting closed segments: %v", err)
	}

	if got, exp := len(files), 0; got != exp {
		t.Fatalf("close segment length mismatch: got %v, exp %v", got, exp)
	}

	if err := w.Delete([]string{"cpu"}); err != nil {
		t.Fatalf("error writing points: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("error closing wal: %v", err)
	}

	// Re-open the WAL
	w = tsm1.NewWAL(dir)
	defer w.Close()
	if err := w.Open(); err != nil {
		t.Fatalf("error opening WAL: %v", err)
	}

	files, err = w.ClosedSegments()
	if err != nil {
		t.Fatalf("error getting closed segments: %v", err)
	}
	if got, exp := len(files), 1; got != exp {
		t.Fatalf("close segment length mismatch: got %v, exp %v", got, exp)
	}
}

func BenchmarkWALSegmentWriter(b *testing.B) {
	points := make([]models.Point, 5000)
	for i := range points {
		points[i] = parsePoint(fmt.Sprintf("cpu,host=host-%d value=1.1 1000000000", i))
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	f := MustTempFile(dir)
	w := tsm1.NewWALSegmentWriter(f)

	write := &tsm1.WriteWALEntry{
		Points: points,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Write(write); err != nil {
			b.Fatalf("unexpected error writing entry: %v", err)
		}
	}
}

func BenchmarkWALSegmentReader(b *testing.B) {
	points := make([]models.Point, 5000)
	for i := range points {
		points[i] = parsePoint(fmt.Sprintf("cpu,host=host-%d value=1.1 1000000000", i))
	}

	dir := MustTempDir()
	defer os.RemoveAll(dir)

	f := MustTempFile(dir)
	w := tsm1.NewWALSegmentWriter(f)

	write := &tsm1.WriteWALEntry{
		Points: points,
	}

	for i := 0; i < 100; i++ {
		if err := w.Write(write); err != nil {
			b.Fatalf("unexpected error writing entry: %v", err)
		}
	}

	r := tsm1.NewWALSegmentReader(f)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		f.Seek(0, os.SEEK_SET)
		b.StartTimer()

		for r.Next() {
			_, err := r.Read()
			if err != nil {
				b.Fatalf("unexpected error reading entry: %v", err)
			}
		}
	}
}
