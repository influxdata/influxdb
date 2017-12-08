package tar

import (
	"archive/tar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func StreamFunc(w io.Writer, dir, relativePath string, writeFunc func(f os.FileInfo, shardRelativePath, fullPath string, tw *tar.Writer) error) error {
	tw := tar.NewWriter(w)
	defer tw.Close()

	return filepath.Walk(dir, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip adding an entry for the root dir
		if dir == path && f.IsDir() {
			return nil
		}

		// Figure out the the full relative path including any sub-dirs
		subDir := strings.TrimPrefix(path, dir+string(filepath.Separator))
		subDir = strings.TrimSuffix(subDir, f.Name())

		return writeFunc(f, filepath.Join(relativePath, subDir), path, tw)
	})
}

// Stream writes a tar archive to w, containing the files and sub-directories in the directory dir
// stored relative to relativePath.
func Stream(w io.Writer, dir, relativePath string) error {
	return StreamFunc(w, dir, relativePath, TarFile)
}

func SinceFilterTarFile(since time.Time) func(f os.FileInfo, shardRelativePath, fullPath string, tw *tar.Writer) error {
	return func(f os.FileInfo, shardRelativePath, fullPath string, tw *tar.Writer) error {
		if f.ModTime().After(since) {
			return TarFile(f, shardRelativePath, fullPath, tw)
		}
		return nil
	}
}

func TarFile(f os.FileInfo, shardRelativePath, fullPath string, tw *tar.Writer) error {
	h, err := tar.FileInfoHeader(f, f.Name())
	if err != nil {
		return err
	}
	h.Name = filepath.Join(shardRelativePath, f.Name())

	if err := tw.WriteHeader(h); err != nil {
		return err
	}

	if !f.Mode().IsRegular() {
		return nil
	}

	fr, err := os.Open(fullPath)
	if err != nil {
		return err
	}

	defer fr.Close()

	_, err = io.CopyN(tw, fr, h.Size)

	return err
}

// Restore reads a tar archive from r and extracts all of its files into dir,
// using only the base name of each file.
func Restore(r io.Reader, dir string) error {
	tr := tar.NewReader(r)
	for {
		if err := extractFile(tr, dir); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}

	return syncDir(dir)
}

// extractFile copies the next file from tr into dir, using the file's base name.
func extractFile(tr *tar.Reader, dir string) error {
	// Read next archive file.
	hdr, err := tr.Next()
	if err != nil {
		return err
	}

	// The hdr.Name is the relative path of the file from the root data dir.
	// e.g (db/rp/1/xxxxx.tsm or db/rp/1/index/xxxxxx.tsi)
	sections := strings.Split(hdr.Name, string(filepath.Separator))
	if len(sections) < 3 {
		return fmt.Errorf("invalid archive path: %s", hdr.Name)
	}

	relativePath := filepath.Join(sections[3:]...)

	subDir, _ := filepath.Split(relativePath)
	// If this is a directory entry (usually just `index` for tsi), create it an move on.
	if hdr.Typeflag == tar.TypeDir {
		if err := os.MkdirAll(filepath.Join(dir, subDir), os.FileMode(hdr.Mode).Perm()); err != nil {
			return err
		}
		return nil
	}

	// Make sure the dir we need to write into exists.  It should, but just double check in
	// case we get a slightly invalid tarball.
	if subDir != "" {
		if err := os.MkdirAll(filepath.Join(dir, subDir), 0755); err != nil {
			return err
		}
	}

	destPath := filepath.Join(dir, relativePath)
	tmp := destPath + ".tmp"

	// Create new file on disk.
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR, os.FileMode(hdr.Mode).Perm())
	if err != nil {
		return err
	}
	defer f.Close()

	// Copy from archive to the file.
	if _, err := io.CopyN(f, tr, hdr.Size); err != nil {
		return err
	}

	// Sync to disk & close.
	if err := f.Sync(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return renameFile(tmp, destPath)
}
