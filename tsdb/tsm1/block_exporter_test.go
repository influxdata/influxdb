package tsm1

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestSQLBlockExporter_Export(t *testing.T) {
	dir := mustTempDir()
	defer os.RemoveAll(dir)
	f := mustTempFile(dir)

	// Write data.
	if w, err := NewTSMWriter(f); err != nil {
		t.Fatal(err)
	} else if err := w.Write([]byte("cpu"), []Value{NewValue(0, int64(1))}); err != nil {
		t.Fatal(err)
	} else if err := w.Write([]byte("mem"), []Value{NewValue(0, int64(2))}); err != nil {
		t.Fatal(err)
	} else if err := w.WriteIndex(); err != nil {
		t.Fatal(err)
	} else if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Expected output.
	want := fmt.Sprintf(`
BEGIN TRANSACTION;
INSERT INTO blocks (filename, org_id, bucket_id, key, type, min_time, max_time, checksum, count) VALUES ('%s', 0, 0, 'cpu', 'integer', 0, 0, 3294968665, 1);
INSERT INTO blocks (filename, org_id, bucket_id, key, type, min_time, max_time, checksum, count) VALUES ('%s', 0, 0, 'mem', 'integer', 0, 0, 755408492, 1);
COMMIT;
`[1:], f.Name(), f.Name())

	// Export file to SQL.
	var buf bytes.Buffer
	e := NewSQLBlockExporter(&buf)
	e.ShowSchema = false
	if err := e.ExportFile(f.Name()); err != nil {
		t.Fatal(err)
	} else if err := e.Close(); err != nil {
		t.Fatal(err)
	} else if got := buf.String(); got != want {
		t.Fatalf("unexpected output:\ngot=%s\n--\nwant=%s", got, want)
	}
}
