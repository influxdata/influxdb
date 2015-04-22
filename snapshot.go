package influxdb

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	"github.com/boltdb/bolt"
)

// manifestName is the name of the manifest file in the snapshot.
const manifestName = "manifest"

// Snapshot represents the state of the Server at a given time.
type Snapshot struct {
	Files []SnapshotFile `json:"files"`
}

// Index returns the highest index across all files.
func (s *Snapshot) Index() uint64 {
	var index uint64
	for _, f := range s.Files {
		if f.Index > index {
			index = f.Index
		}
	}
	return index
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

	// Sort files.
	sort.Sort(SnapshotFiles(diff.Files))

	return diff
}

// Merge returns a Snapshot that combines s with other.
// Only the newest file between the two snapshots is returned.
func (s *Snapshot) Merge(other *Snapshot) *Snapshot {
	ret := &Snapshot{}
	ret.Files = make([]SnapshotFile, len(s.Files))
	copy(ret.Files, s.Files)

	// Update/insert versions of files that are newer in other.
loop:
	for _, a := range other.Files {
		for i, b := range ret.Files {
			// Ignore if it doesn't match.
			if a.Name != b.Name {
				continue
			}

			// Update if it's newer and then start the next file.
			if a.Index > b.Index {
				ret.Files[i] = a
			}
			continue loop
		}

		// If the file wasn't found then append it.
		ret.Files = append(ret.Files, a)
	}

	// Sort files.
	sort.Sort(SnapshotFiles(ret.Files))

	return ret
}

// SnapshotFile represents a single file in a Snapshot.
type SnapshotFile struct {
	Name  string `json:"name"`  // filename
	Size  int64  `json:"size"`  // file size
	Index uint64 `json:"index"` // highest index applied
}

// SnapshotFiles represents a sortable list of snapshot files.
type SnapshotFiles []SnapshotFile

func (p SnapshotFiles) Len() int           { return len(p) }
func (p SnapshotFiles) Less(i, j int) bool { return p[i].Name < p[j].Name }
func (p SnapshotFiles) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// SnapshotReader reads a snapshot from a Reader.
// This type is not safe for concurrent use.
type SnapshotReader struct {
	tr       *tar.Reader
	snapshot *Snapshot
}

// NewSnapshotReader returns a new SnapshotReader reading from r.
func NewSnapshotReader(r io.Reader) *SnapshotReader {
	return &SnapshotReader{
		tr: tar.NewReader(r),
	}
}

// Snapshot returns the snapshot meta data.
func (sr *SnapshotReader) Snapshot() (*Snapshot, error) {
	if err := sr.readSnapshot(); err != nil {
		return nil, err
	}
	return sr.snapshot, nil
}

// readSnapshot reads the first entry from the snapshot and materializes the snapshot.
// This is skipped if the snapshot manifest has already been read.
func (sr *SnapshotReader) readSnapshot() error {
	// Already read, ignore.
	if sr.snapshot != nil {
		return nil
	}

	// Read manifest header.
	hdr, err := sr.tr.Next()
	if err != nil {
		return fmt.Errorf("snapshot header: %s", err)
	} else if hdr.Name != manifestName {
		return fmt.Errorf("invalid snapshot header: expected manifest")
	}

	// Materialize snapshot.
	var snapshot Snapshot
	if err := json.NewDecoder(sr.tr).Decode(&snapshot); err != nil {
		return fmt.Errorf("decode manifest: %s", err)
	}
	sr.snapshot = &snapshot

	return nil
}

// Next returns the next file in the snapshot.
func (sr *SnapshotReader) Next() (SnapshotFile, error) {
	// Read snapshot if it hasn't been read yet.
	if err := sr.readSnapshot(); err != nil {
		return SnapshotFile{}, err
	}

	// Read next header.
	hdr, err := sr.tr.Next()
	if err != nil {
		return SnapshotFile{}, err
	}

	// Match header to file in snapshot.
	for i := range sr.snapshot.Files {
		if sr.snapshot.Files[i].Name == hdr.Name {
			return sr.snapshot.Files[i], nil
		}
	}

	// Return error if file is not in the snapshot.
	return SnapshotFile{}, fmt.Errorf("snapshot entry not found in manifest: %s", hdr.Name)
}

// Read reads the current entry in the snapshot.
func (sr *SnapshotReader) Read(b []byte) (n int, err error) {
	// Read snapshot if it hasn't been read yet.
	if err := sr.readSnapshot(); err != nil {
		return 0, err
	}

	// Pass read through to the tar reader.
	return sr.tr.Read(b)
}

// SnapshotsReader reads from a collection of snapshots.
// Only files with the highest index are read from the reader.
// This type is not safe for concurrent use.
type SnapshotsReader struct {
	readers []*SnapshotReader // underlying snapshot readers
	files   []*SnapshotFile   // current file for each reader

	snapshot *Snapshot       // combined snapshot from all readers
	index    int             // index of file in snapshot to read
	curr     *SnapshotReader // current reader
}

// NewSnapshotsReader returns a new SnapshotsReader reading from a list of readers.
func NewSnapshotsReader(readers ...io.Reader) *SnapshotsReader {
	r := &SnapshotsReader{
		readers: make([]*SnapshotReader, len(readers)),
		files:   make([]*SnapshotFile, len(readers)),
		index:   -1,
	}
	for i := range readers {
		r.readers[i] = NewSnapshotReader(readers[i])
	}
	return r
}

// Snapshot returns the combined snapshot from all readers.
func (ssr *SnapshotsReader) Snapshot() (*Snapshot, error) {
	// Use snapshot if it's already been calculated.
	if ssr.snapshot != nil {
		return ssr.snapshot, nil
	}

	// Build snapshot from other readers.
	ss := &Snapshot{}
	for i, sr := range ssr.readers {
		other, err := sr.Snapshot()
		if err != nil {
			return nil, fmt.Errorf("snapshot: idx=%d, err=%s", i, err)
		}
		ss = ss.Merge(other)
	}

	// Cache snapshot and return.
	ssr.snapshot = ss
	return ss, nil
}

// Next returns the next file in the reader.
func (ssr *SnapshotsReader) Next() (SnapshotFile, error) {
	ss, err := ssr.Snapshot()
	if err != nil {
		return SnapshotFile{}, fmt.Errorf("snapshot: %s", err)
	}

	// Return EOF if there are no more files in snapshot.
	if ssr.index == len(ss.Files)-1 {
		ssr.curr = nil
		return SnapshotFile{}, io.EOF
	}

	// Queue up next files.
	if err := ssr.nextFiles(); err != nil {
		return SnapshotFile{}, fmt.Errorf("next files: %s", err)
	}

	// Increment the file index.
	ssr.index++
	sf := ss.Files[ssr.index]

	// Find the matching reader. Clear other readers.
	var sr *SnapshotReader
	for i, f := range ssr.files {
		if f == nil || f.Name != sf.Name {
			continue
		}

		// Set reader to the first match.
		if sr == nil && *f == sf {
			sr = ssr.readers[i]
		}
		ssr.files[i] = nil
	}

	// Return an error if file doesn't match.
	// This shouldn't happen unless the underlying snapshot is altered.
	if sr == nil {
		return SnapshotFile{}, fmt.Errorf("snaphot file not found in readers: %s", sf.Name)
	}

	// Set current reader.
	ssr.curr = sr

	// Return file.
	return sf, nil
}

// nextFiles queues up a next file for all readers.
func (ssr *SnapshotsReader) nextFiles() error {
	for i, sr := range ssr.readers {
		if ssr.files[i] == nil {
			// Read next file.
			sf, err := sr.Next()
			if err == io.EOF {
				ssr.files[i] = nil
				continue
			} else if err != nil {
				return fmt.Errorf("next: reader=%d, err=%s", i, err)
			}

			// Cache file.
			ssr.files[i] = &sf
		}
	}

	return nil
}

// nextIndex returns the index of the next reader to read from.
// Returns -1 if all readers are at EOF.
func (ssr *SnapshotsReader) nextIndex() int {
	// Find the next file by name and lowest index.
	index := -1
	for i, f := range ssr.files {
		if f == nil {
			continue
		} else if index == -1 {
			index = i
		} else if f.Name < ssr.files[index].Name {
			index = i
		} else if f.Name == ssr.files[index].Name && f.Index > ssr.files[index].Index {
			index = i
		}
	}
	return index
}

// Read reads the current entry in the reader.
func (ssr *SnapshotsReader) Read(b []byte) (n int, err error) {
	if ssr.curr == nil {
		return 0, io.EOF
	}
	return ssr.curr.Read(b)
}

// OpenFileSnapshotsReader returns a SnapshotsReader based on the path of the base snapshot.
// Returns the underlying files which need to be closed separately.
func OpenFileSnapshotsReader(path string) (*SnapshotsReader, []io.Closer, error) {
	var readers []io.Reader
	var closers []io.Closer
	if err := func() error {
		// Open original snapshot file.
		f, err := os.Open(path)
		if os.IsNotExist(err) {
			return err
		} else if err != nil {
			return fmt.Errorf("open snapshot: %s", err)
		}
		readers = append(readers, f)
		closers = append(closers, f)

		// Open all incremental snapshots.
		for i := 0; ; i++ {
			filename := path + fmt.Sprintf(".%d", i)
			f, err := os.Open(filename)
			if os.IsNotExist(err) {
				break
			} else if err != nil {
				return fmt.Errorf("open incremental snapshot: file=%s, err=%s", filename, err)
			}
			readers = append(readers, f)
			closers = append(closers, f)
		}

		return nil
	}(); err != nil {
		closeAll(closers)
		return nil, nil, err
	}

	return NewSnapshotsReader(readers...), nil, nil
}

// ReadFileSnapshot returns a Snapshot for a given base snapshot path.
// This snapshot merges all incremental backup snapshots as well.
func ReadFileSnapshot(path string) (*Snapshot, error) {
	// Open a multi-snapshot reader.
	ssr, files, err := OpenFileSnapshotsReader(path)
	if os.IsNotExist(err) {
		return nil, err
	} else if err != nil {
		return nil, fmt.Errorf("open file snapshots reader: %s", err)
	}
	defer closeAll(files)

	// Read snapshot.
	ss, err := ssr.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("snapshot: %s", err)
	}

	return ss, nil
}

func closeAll(a []io.Closer) {
	for _, c := range a {
		_ = c.Close()
	}
}

// SnapshotWriter writes a snapshot and the underlying files to disk as a tar archive.
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

	// Sort snapshot files.
	// This is required for combining multiple snapshots together.
	sort.Sort(SnapshotFiles(sw.Snapshot.Files))

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
		Name:    manifestName,
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
		return nil, nil, fmt.Errorf("begin - stats %s, err %s", sh.stats, err)
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
