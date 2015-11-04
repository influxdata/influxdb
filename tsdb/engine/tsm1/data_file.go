package tsm1

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type DataFiles struct {
	path string

	files         dataFiles
	mu            sync.RWMutex
	currentFileID int
}

func (d *DataFiles) Open() error {
	files, err := filepath.Glob(filepath.Join(d.path, fmt.Sprintf("*.%s", Format)))
	if err != nil {
		return err
	}
	for _, fn := range files {
		// if the file has a checkpoint it's not valid, so remove it
		if removed := d.removeFileIfCheckpointExists(fn); removed {
			continue
		}

		id, err := idFromFileName(fn)
		if err != nil {
			return err
		}
		if id >= d.currentFileID {
			d.currentFileID = id + 1
		}
		f, err := os.OpenFile(fn, os.O_RDONLY, 0666)
		if err != nil {
			return fmt.Errorf("error opening file %s: %s", fn, err.Error())
		}
		df := NewDataFile(f)
		d.files = append(d.files, df)
	}
	sort.Sort(d.files)
	return nil
}

func (d *DataFiles) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, df := range d.files {
		_ = df.Close()
	}

	d.files = nil
	d.currentFileID = 0
	return nil
}

func (d *DataFiles) NextFileName() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.currentFileID++
	return filepath.Join(d.path, fmt.Sprintf("%07d.%s", d.currentFileID, Format))
}

// DataFileCount returns the number of data files in the database
func (d *DataFiles) Count() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.files)
}

func (d *DataFiles) Add(files dataFiles) {
	// update the engine to point at the new dataFiles
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, fr := range files {
		exists := false

		for _, df := range d.files {

			if df == fr {
				exists = true
				break
			}
		}
		if !exists {
			d.files = append(d.files, fr)
		}
	}

	sort.Sort(d.files)
}

func (d *DataFiles) Remove(files dataFiles) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var newFiles []*dataFile
	for _, df := range d.files {
		removeFile := false

		for _, fr := range files {
			if df == fr {
				removeFile = true
				break
			}
		}

		if !removeFile {
			newFiles = append(newFiles, df)
		}
	}

	sort.Sort(dataFiles(newFiles))
	d.files = newFiles
}

func (d *DataFiles) Copy() []*dataFile {
	d.mu.RLock()
	defer d.mu.RUnlock()
	a := make([]*dataFile, len(d.files))
	copy(a, d.files)
	return a
}

func (d *DataFiles) Replace(files dataFiles) {
	// update the delete map and files
	d.mu.Lock()
	defer d.mu.Unlock()

	d.files = files
}

func (d *DataFiles) Overlapping(min, max int64) []*dataFile {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var a []*dataFile
	for _, f := range d.files {
		fmin, fmax := f.MinTime(), f.MaxTime()
		if min < fmax && fmin >= fmin {
			a = append(a, f)
		} else if max >= fmin && max < fmax {
			a = append(a, f)
		}
	}
	for _, f := range a {
		f.Reference()
	}
	return a
}

func (d *DataFiles) Compactable(age time.Duration, size uint32) dataFiles {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var a dataFiles
	for _, df := range d.files {
		if time.Since(df.modTime) > age && df.size < size {
			a = append(a, df)
		} else if len(a) > 0 {
			// only compact contiguous ranges. If we hit the negative case and
			// there are files to compact, stop here
			break
		}
	}
	return a

}

// removeFileIfCheckpointExists will remove the file if its associated checkpoint fil is there.
// It returns true if the file was removed. This is for recovery of data files on startup
func (d *DataFiles) removeFileIfCheckpointExists(fileName string) bool {
	checkpointName := fmt.Sprintf("%s.%s", fileName, CheckpointExtension)
	_, err := os.Stat(checkpointName)

	// if there's no checkpoint, move on
	if err != nil {
		return false
	}

	// there's a checkpoint so we know this file isn't safe so we should remove it
	err = os.RemoveAll(fileName)
	if err != nil {
		panic(fmt.Sprintf("error removing file %s", err.Error()))
	}

	err = os.RemoveAll(checkpointName)
	if err != nil {
		panic(fmt.Sprintf("error removing file %s", err.Error()))
	}

	return true
}
