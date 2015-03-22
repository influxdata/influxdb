package influxdb

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"
)

// Snapshot represents the state of the Server at a given time.
type Snapshot struct {
	Files []SnapshotFile `json:"files"`
}

// Diff returns a Snapshot of files that are newer in s than other.
func (s *Snapshot) Diff(other *Snapshot) *Snapshot {
	diff := &Snapshot{}

	// Find versions of files that are newer in s.
loop:
	for _, a := range s.Files {
		// Try to find a newer version of the file in other.
		// If found then don't append this file and move to the next file.
		for _, b := range other.Files {
			if a.Name != b.Name {
				continue
			} else if a.Index <= b.Index {
				continue loop
			} else {
				break
			}
		}

		// Append the newest version.
		diff.Files = append(diff.Files, a)
	}

	return diff
}

// SnapshotFile represents a single file in a Snapshot.
type SnapshotFile struct {
	Name  string `json:"name"`  // filename
	Size  int64  `json:"size"`  // file size
	Index uint64 `json:"index"` // highest index applied
}

// SnapshotWriter writes a snapshot and the underlying files to disk as a tar archive.
// This type is not safe for concurrent use.
type SnapshotWriter struct {
	// The snapshot to write from.
	// Removing files from the snapshot after creation will cause those files to be ignored.
	Snapshot *Snapshot

	// Writers for each file by filename.
	// Writers will be closed as they're processed and will close by the end of WriteTo().
	FileWriters map[string]SnapshotFileWriter
}

// NewSnapshotWriter returns a new instance of SnapshotWriter.
func NewSnapshotWriter() *SnapshotWriter {
	return &SnapshotWriter{
		Snapshot:    &Snapshot{},
		FileWriters: make(map[string]SnapshotFileWriter),
	}
}

// Close closes all file writers on the snapshot.
func (sw *SnapshotWriter) Close() error {
	for _, fw := range sw.FileWriters {
		_ = fw.Close()
	}
	return nil
}

// closeUnusedWriters closes all file writers not on the snapshot.
// This allows transactions on these files to be short lived.
func (sw *SnapshotWriter) closeUnusedWriters() {
loop:
	for name, fw := range sw.FileWriters {
		// Find writer in snapshot.
		for _, f := range sw.Snapshot.Files {
			if f.Name == name {
				continue loop
			}
		}

		// If not found then close it.
		_ = fw.Close()
	}
}

// WriteTo writes the snapshot to the writer.
// File writers are closed as they are written.
// This function will always return n == 0.
func (sw *SnapshotWriter) WriteTo(w io.Writer) (n int64, err error) {
	// Close any file writers that aren't required.
	sw.closeUnusedWriters()

	// Begin writing a tar file to the output.
	tw := tar.NewWriter(w)
	defer tw.Close()

	// Write manifest file.
	if err := sw.writeManifestTo(tw); err != nil {
		return 0, fmt.Errorf("write manifest: %s", err)
	}

	// Write each backup file.
	for _, f := range sw.Snapshot.Files {
		if err := sw.writeFileTo(tw, &f); err != nil {
			return 0, fmt.Errorf("write file: %s", err)
		}
	}

	// Close tar writer and check error.
	if err := tw.Close(); err != nil {
		return 0, fmt.Errorf("tar close: %s", err)
	}

	return 0, nil
}

// writeManifestTo writes a manifest of the contents of the snapshot to the archive.
func (sw *SnapshotWriter) writeManifestTo(tw *tar.Writer) error {
	// Convert snapshot to JSON.
	b, err := json.Marshal(sw.Snapshot)
	if err != nil {
		return fmt.Errorf("marshal json: %s", err)
	}

	// Write header & file.
	if err := tw.WriteHeader(&tar.Header{
		Name:    "manifest",
		Size:    int64(len(b)),
		Mode:    0666,
		ModTime: time.Now(),
	}); err != nil {
		return fmt.Errorf("write header: %s", err)
	}
	if _, err := tw.Write(b); err != nil {
		return fmt.Errorf("write: %s", err)
	}

	return nil
}

// writeFileTo writes a single file to the archive.
func (sw *SnapshotWriter) writeFileTo(tw *tar.Writer, f *SnapshotFile) error {
	// Retrieve the file writer by filename.
	fw := sw.FileWriters[f.Name]
	if fw == nil {
		return fmt.Errorf("file writer not found: name=%s", f.Name)
	}

	// Write file header.
	if err := tw.WriteHeader(&tar.Header{
		Name:    f.Name,
		Size:    f.Size,
		Mode:    0666,
		ModTime: time.Now(),
	}); err != nil {
		return fmt.Errorf("write header: file=%s, err=%s", f.Name, err)
	}

	// Copy the database to the writer.
	if nn, err := fw.WriteTo(tw); err != nil {
		return fmt.Errorf("write: file=%s, err=%s", f.Name, err)
	} else if nn != f.Size {
		return fmt.Errorf("short write: file=%s", f.Name)
	}

	// Close the writer.
	if err := fw.Close(); err != nil {
		return fmt.Errorf("close: file=%s, err=%s", f.Name, err)
	}

	return nil
}

// createServerSnapshotWriter creates a snapshot writer from a locked server.
func createServerSnapshotWriter(s *Server) (*SnapshotWriter, error) {
	// Exit if the server is closed.
	if !s.opened() {
		return nil, ErrServerClosed
	}

	// Create snapshot writer.
	sw := NewSnapshotWriter()

	if err := func() error {
		f, fw, err := createMetaSnapshotFile(s.meta)
		if err != nil {
			return fmt.Errorf("create meta snapshot file: %s", err)
		}
		sw.Snapshot.Files = append(sw.Snapshot.Files, *f)
		sw.FileWriters[f.Name] = fw

		// Create files for each shard.
		for _, sh := range s.shards {
			f, fw, err := createShardSnapshotFile(sh)
			if err != nil {
				return fmt.Errorf("create meta snapshot file: id=%d, err=%s", sh.ID, err)
			} else if f != nil {
				sw.Snapshot.Files = append(sw.Snapshot.Files, *f)
				sw.FileWriters[f.Name] = fw
			}
		}

		return nil
	}(); err != nil {
		_ = sw.Close()
		return nil, err
	}

	return sw, nil
}

func createMetaSnapshotFile(meta *metastore) (*SnapshotFile, SnapshotFileWriter, error) {
	// Begin transaction.
	tx, err := meta.db.Begin(false)
	if err != nil {
		return nil, nil, fmt.Errorf("begin: %s", err)
	}

	// Create and return file and writer.
	f := &SnapshotFile{
		Name:  "meta",
		Size:  tx.Size(),
		Index: (&metatx{tx}).index(),
	}
	return f, &boltTxCloser{tx}, nil
}

func createShardSnapshotFile(sh *Shard) (*SnapshotFile, SnapshotFileWriter, error) {
	// Ignore shard if it's not owned by the server.
	if sh.store == nil {
		return nil, nil, nil
	}

	// Begin transaction.
	tx, err := sh.store.Begin(false)
	if err != nil {
		return nil, nil, fmt.Errorf("begin: %s", err)
	}

	// Create and return file and writer.
	f := &SnapshotFile{
		Name:  path.Join("shards", filepath.Base(sh.store.Path())),
		Size:  tx.Size(),
		Index: shardMetaIndex(tx),
	}
	return f, &boltTxCloser{tx}, nil
}

// SnapshotFileWriter is the interface used for writing a file to a snapshot.
type SnapshotFileWriter interface {
	io.WriterTo
	io.Closer
}

// boltTxCloser wraps a Bolt transaction to implement io.Closer.
type boltTxCloser struct {
	*bolt.Tx
}

// Close rollsback the transaction.
func (tx *boltTxCloser) Close() error { return tx.Rollback() }

// NopWriteToCloser returns an io.WriterTo that implements io.Closer.
func NopWriteToCloser(w io.WriterTo) interface {
	io.WriterTo
	io.Closer
} {
	return &nopWriteToCloser{w}
}

type nopWriteToCloser struct {
	io.WriterTo
}

func (w *nopWriteToCloser) Close() error { return nil }
