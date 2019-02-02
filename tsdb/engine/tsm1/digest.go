package tsm1

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	DigestFilename = "digest.tsd"
)

type DigestOptions struct {
	MinTime, MaxTime int64
	MinKey, MaxKey   []byte
}

// DigestWithOptions writes a digest of dir to w using options to filter by
// time and key range.
func DigestWithOptions(dir string, files []string, opts DigestOptions, w io.WriteCloser) error {
	manifest, err := NewDigestManifest(dir, files)
	if err != nil {
		return err
	}

	tsmFiles := make([]TSMFile, 0, len(files))
	defer func() {
		for _, r := range tsmFiles {
			r.Close()
		}
	}()

	readers := make([]*TSMReader, 0, len(files))
	for _, fi := range files {
		f, err := os.Open(fi)
		if err != nil {
			return err
		}

		r, err := NewTSMReader(f)
		if err != nil {
			return err
		}
		readers = append(readers, r)
		tsmFiles = append(tsmFiles, r)
	}

	dw, err := NewDigestWriter(w)
	if err != nil {
		return err
	}
	defer dw.Close()

	// Write the manifest.
	if err := dw.WriteManifest(manifest); err != nil {
		return err
	}

	// Write the digest data.
	var n int
	ki := newMergeKeyIterator(tsmFiles, nil)
	for ki.Next() {
		key, _ := ki.Read()
		if len(opts.MinKey) > 0 && bytes.Compare(key, opts.MinKey) < 0 {
			continue
		}

		if len(opts.MaxKey) > 0 && bytes.Compare(key, opts.MaxKey) > 0 {
			continue
		}

		ts := &DigestTimeSpan{}
		n++
		kstr := string(key)

		for _, r := range readers {
			entries := r.Entries(key)
			for _, entry := range entries {
				crc, b, err := r.ReadBytes(&entry, nil)
				if err != nil {
					return err
				}

				// Filter blocks that are outside the time filter.  If they overlap, we
				// still include them.
				if entry.MaxTime < opts.MinTime || entry.MinTime > opts.MaxTime {
					continue
				}

				cnt := BlockCount(b)
				ts.Add(entry.MinTime, entry.MaxTime, cnt, crc)
			}
		}

		sort.Sort(ts)
		if err := dw.WriteTimeSpan(kstr, ts); err != nil {
			return err
		}
	}
	return dw.Close()
}

// Digest writes a digest of dir to w of a full shard dir.
func Digest(dir string, files []string, w io.WriteCloser) error {
	return DigestWithOptions(dir, files, DigestOptions{
		MinTime: math.MinInt64,
		MaxTime: math.MaxInt64,
	}, w)
}

// DigestFresh returns true if digest cached in dir is still fresh and returns
// false if it is stale. If the digest is stale, a string description of the
// reason is also returned. files is a list of filenames the caller expects the
// digest to contain, usually from the engine's FileStore.
func DigestFresh(dir string, files []string, shardLastMod time.Time) (bool, string) {
	// Open the digest file.
	digestPath := filepath.Join(dir, DigestFilename)
	f, err := os.Open(digestPath)
	if err != nil {
		return false, fmt.Sprintf("Can't open digest file: %s", err)
	}
	defer f.Close()

	// Get digest file info.
	digest, err := f.Stat()
	if err != nil {
		return false, fmt.Sprintf("Can't stat digest file: %s", err)
	}

	// See if shard was modified after digest was generated.
	if shardLastMod.After(digest.ModTime()) {
		return false, fmt.Sprintf("Shard modified: shard_time=%v, digest_time=%v", shardLastMod, digest.ModTime())
	}

	// Read the manifest from the digest file.
	dr, err := NewDigestReader(f)
	if err != nil {
		return false, fmt.Sprintf("Can't read digest: err=%s", err)
	}
	defer dr.Close()

	mfest, err := dr.ReadManifest()
	if err != nil {
		return false, fmt.Sprintf("Can't read manifest: err=%s", err)
	}

	// Make sure the digest file belongs to this shard.
	if mfest.Dir != dir {
		return false, fmt.Sprintf("Digest belongs to another shard. Manually copied?: manifest_dir=%s, shard_dir=%s", mfest.Dir, dir)
	}

	// See if the number of tsm files matches what's listed in the manifest.
	if len(files) != len(mfest.Entries) {
		return false, fmt.Sprintf("Number of tsm files differ: engine=%d, manifest=%d", len(files), len(mfest.Entries))
	}

	// See if all the tsm files match the manifest.
	sort.Strings(files)
	for i, tsmname := range files {
		entry := mfest.Entries[i]

		// Check filename.
		if tsmname != entry.Filename {
			return false, fmt.Sprintf("Names don't match: manifest_entry=%d, engine_name=%s, manifest_name=%s", i, tsmname, entry.Filename)
		}

		// Get tsm file info.
		tsm, err := os.Stat(tsmname)
		if err != nil {
			return false, fmt.Sprintf("Can't stat tsm file: manifest_entry=%d, path=%s", i, tsmname)
		}

		// See if tsm file size has changed.
		if tsm.Size() != entry.Size {
			return false, fmt.Sprintf("TSM file size changed: manifest_entry=%d, path=%s, tsm=%d, manifest=%d", i, tsmname, tsm.Size(), entry.Size)
		}

		// See if tsm file was modified after the digest was created. This should be
		// covered by the engine mod time check above but we'll check each file to
		// be sure. It's better to regenerate the digest than use a stale one.
		if tsm.ModTime().After(digest.ModTime()) {
			return false, fmt.Sprintf("TSM file modified: manifest_entry=%d, path=%s, tsm_time=%v, digest_time=%v", i, tsmname, tsm.ModTime(), digest.ModTime())
		}
	}

	// Digest is fresh.
	return true, ""
}

// DigestManifest contains a list of tsm files used to generate a digest
// and information about those files which can be used to verify the
// associated digest file is still valid.
type DigestManifest struct {
	// Dir is the directory path this manifest describes.
	Dir string `json:"dir"`
	// Entries is a list of files used to generate a digest.
	Entries DigestManifestEntries `json:"entries"`
}

// NewDigestManifest creates a digest manifest for a shard directory and list
// of tsm files from that directory.
func NewDigestManifest(dir string, files []string) (*DigestManifest, error) {
	mfest := &DigestManifest{
		Dir:     dir,
		Entries: make([]*DigestManifestEntry, len(files)),
	}

	for i, name := range files {
		fi, err := os.Stat(name)
		if err != nil {
			return nil, err
		}
		mfest.Entries[i] = NewDigestManifestEntry(name, fi.Size())
	}

	sort.Sort(mfest.Entries)

	return mfest, nil
}

type DigestManifestEntry struct {
	// Filename is the name of one .tsm file used in digest generation.
	Filename string `json:"filename"`
	// Size is the size, in bytes, of the .tsm file.
	Size int64 `json:"size"`
}

// NewDigestManifestEntry creates a digest manifest entry initialized with a
// tsm filename and its size.
func NewDigestManifestEntry(filename string, size int64) *DigestManifestEntry {
	return &DigestManifestEntry{
		Filename: filename,
		Size:     size,
	}
}

// DigestManifestEntries is a list of entries in a manifest file, ordered by
// tsm filename.
type DigestManifestEntries []*DigestManifestEntry

func (a DigestManifestEntries) Len() int           { return len(a) }
func (a DigestManifestEntries) Less(i, j int) bool { return a[i].Filename < a[j].Filename }
func (a DigestManifestEntries) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
