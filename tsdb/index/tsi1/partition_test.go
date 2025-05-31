package tsi1_test

import (
	"errors"
	"fmt"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestPartition_Open(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	// Opening a fresh index should set the MANIFEST version to current version.
	p := NewPartition(sfile.SeriesFile)
	t.Run("open new index", func(t *testing.T) {
		if err := p.Open(); err != nil {
			t.Fatal(err)
		}

		// Check version set appropriately.
		if got, exp := p.Manifest().Version, 1; got != exp {
			t.Fatalf("got index version %d, expected %d", got, exp)
		}
	})

	// Reopening an open index should return an error.
	t.Run("reopen open index", func(t *testing.T) {
		err := p.Open()
		if err == nil {
			p.Close()
			t.Fatal("didn't get an error on reopen, but expected one")
		}
		p.Close()
	})

	// Opening an incompatible index should return an error.
	incompatibleVersions := []int{-1, 0, 2}
	for _, v := range incompatibleVersions {
		t.Run(fmt.Sprintf("incompatible index version: %d", v), func(t *testing.T) {
			p = NewPartition(sfile.SeriesFile)
			// Manually create a MANIFEST file for an incompatible index version.
			mpath := filepath.Join(p.Path(), tsi1.ManifestFileName)
			m := tsi1.NewManifest(mpath)
			m.Levels = nil
			m.Version = v // Set example MANIFEST version.
			if _, err := m.Write(); err != nil {
				t.Fatal(err)
			}

			// Log the MANIFEST file.
			data, err := os.ReadFile(mpath)
			if err != nil {
				panic(err)
			}
			t.Logf("Incompatible MANIFEST: %s", data)

			// Opening this index should return an error because the MANIFEST has an
			// incompatible version.
			err = p.Open()
			if !errors.Is(err, tsi1.ErrIncompatibleVersion) {
				p.Close()
				t.Fatalf("got error %v, expected %v", err, tsi1.ErrIncompatibleVersion)
			}
		})
	}
}

func TestPartition_Manifest(t *testing.T) {
	t.Run("current MANIFEST", func(t *testing.T) {
		sfile := MustOpenSeriesFile()
		t.Cleanup(func() {
			if err := sfile.Close(); err != nil {
				t.Fatalf("error closing series file %v", err)
			}
		})

		p := MustOpenPartition(sfile.SeriesFile)
		t.Cleanup(func() {
			if err := p.Close(); err != nil {
				t.Fatalf("error closing partition %v", err)
			}
		})

		if got, exp := p.Manifest().Version, tsi1.Version; got != exp {
			t.Fatalf("got MANIFEST version %d, expected %d", got, exp)
		}
	})
}

var badManifestPath string = filepath.Join(os.DevNull, tsi1.ManifestFileName)

func TestPartition_Manifest_Write_Fail(t *testing.T) {
	t.Run("write MANIFEST", func(t *testing.T) {
		m := tsi1.NewManifest(badManifestPath)
		_, err := m.Write()
		if !errors.Is(err, syscall.ENOTDIR) {
			t.Fatalf("expected: syscall.ENOTDIR, got %T: %v", err, err)
		}
	})
}

func TestPartition_PrependLogFile_Write_Fail(t *testing.T) {
	t.Run("write MANIFEST", func(t *testing.T) {
		sfile := MustOpenSeriesFile()
		t.Cleanup(func() {
			if err := sfile.Close(); err != nil {
				t.Fatalf("error closing series file %v", err)
			}
		})
		p := MustOpenPartition(sfile.SeriesFile)
		t.Cleanup(func() {
			if err := p.Close(); err != nil {
				t.Fatalf("error closing partition: %v", err)
			}
		})
		p.Partition.SetMaxLogFileSize(-1)
		fileN := p.FileN()
		p.CheckLogFile()
		if fileN >= p.FileN() {
			t.Fatalf("manifest write prepending log file should have succeeded but number of files did not change correctly: expected more than %d files, got %d files", fileN, p.FileN())
		}
		p.SetManifestPathForTest(badManifestPath)
		fileN = p.FileN()
		p.CheckLogFile()
		if fileN != p.FileN() {
			t.Fatalf("manifest write prepending log file should have failed, but number of files changed: expected %d files, got %d files", fileN, p.FileN())
		}
	})
}

func TestPartition_Compact_Write_Fail(t *testing.T) {
	t.Run("write MANIFEST", func(t *testing.T) {
		sfile := MustOpenSeriesFile()
		t.Cleanup(func() {
			if err := sfile.Close(); err != nil {
				t.Fatalf("error closing series file %v", err)
			}
		})

		p := MustOpenPartition(sfile.SeriesFile)
		t.Cleanup(func() {
			if err := p.Close(); err != nil {
				t.Fatalf("error closing partition: %v", err)
			}
		})
		p.Partition.SetMaxLogFileSize(-1)
		fileN := p.FileN()
		p.Compact()
		if (1 + fileN) != p.FileN() {
			t.Fatalf("manifest write in compaction should have succeeded, but number of files did not change correctly: expected %d files, got %d files", fileN+1, p.FileN())
		}
		p.SetManifestPathForTest(badManifestPath)
		fileN = p.FileN()
		p.Compact()
		if fileN != p.FileN() {
			t.Fatalf("manifest write should have failed the compaction, but number of files changed: expected %d files, got %d files", fileN, p.FileN())
		}
	})
}

// Test case for https://github.com/influxdata/plutonium/issues/4217
func TestPartition_Compact_Deadlock(t *testing.T) {
	// TODO: Generate some log files (level0) and some index files
	// I'll need to try and compact these files to the extent where it
	// causes a deadlock as shown by the following better go playground
	// https://goplay.tools/snippet/QKHQQPTBJUZ
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	p := NewPartition(sfile.SeriesFile)
	t.Run("open new index", func(t *testing.T) {
		if err := p.Open(); err != nil {
			t.Fatal(err)
		}

		if got, exp := p.Manifest().Version, 1; got != exp {
			t.Fatalf("got index version %d, expected %d", got, exp)
		}
	})

	t.Run("reopen open index", func(t *testing.T) {
		err := p.Open()
		if err == nil {
			err := p.Close()
			require.NoError(t, err, "error closing partition")
			t.Fatal("didn't get an error on reopen, but expected one")
		}
		err = p.Close()
		require.NoError(t, err, "error closing partition")
	})

	t.Run("check for deadlocks while compacting tsl and tsi files", func(t *testing.T) {
		p = NewPartition(sfile.SeriesFile)
		err := p.Open()
		require.NoError(t, err, "open new index")

		var allFiles []string

		fmt.Println("Creating 100 TSL files...")
		for fileNum := 0; fileNum < 100; fileNum++ {
			tempPath := filepath.Join(p.Path(), fmt.Sprintf(".temp_tsl_%d", fileNum))

			tsiIndex := tsi1.NewIndex(sfile.SeriesFile, "",
				tsi1.WithPath(tempPath),
				tsi1.WithMaximumLogFileSize(1024*1024*1024), // Very large - prevent compaction
				tsi1.WithLogFileBufferSize(4*1024),
				tsi1.DisableFsync(),
			)

			if err := tsiIndex.Open(); err != nil {
				t.Fatal(err)
			}

			seriesPerFile := 300
			startIdx := fileNum * seriesPerFile
			for i := startIdx; i < startIdx+seriesPerFile; i++ {
				measurement := fmt.Sprintf("tsl_measurement_%d", i)
				seriesKey := fmt.Sprintf("%s,host=server%d,tslfile=%d", measurement, i%20, fileNum)
				tags := models.ParseTags([]byte(fmt.Sprintf("host=server%d,tslfile=%d", i%20, fileNum)))

				if err := tsiIndex.CreateSeriesIfNotExists([]byte(seriesKey), []byte(measurement), tags, tsdb.NoopStatsTracker()); err != nil {
					t.Fatal(err)
				}
			}

			err = tsiIndex.Close()
			require.NoError(t, err, "error closing index")

			tempFiles, _ := filepath.Glob(filepath.Join(tempPath, "*", "*.tsl"))
			if len(tempFiles) > 0 {
				newName := fmt.Sprintf("L0-%08d.tsl", fileNum)
				dstPath := filepath.Join(p.Path(), newName)
				os.Rename(tempFiles[0], dstPath)
				allFiles = append(allFiles, newName)
			}

			os.RemoveAll(tempPath)

			if fileNum%10 == 0 {
				fmt.Printf("Created %d TSL files...\n", fileNum+1)
			}
		}

		fmt.Println("Creating 200+ TSI files...")
		tsiFileNum := 0

		for batch := 0; batch < 100; batch++ { // 100 separate index instances
			tempPath := filepath.Join(p.Path(), fmt.Sprintf(".temp_tsi_batch_%d", batch))

			tsiIndex := tsi1.NewIndex(sfile.SeriesFile, "",
				tsi1.WithPath(tempPath),
				tsi1.WithMaximumLogFileSize(2*1024),
				tsi1.WithLogFileBufferSize(1*1024),
				tsi1.DisableFsync(),
			)

			if err := tsiIndex.Open(); err != nil {
				t.Fatal(err)
			}

			for round := 0; round < 3; round++ {
				batchSize := 200
				keysBatch := make([][]byte, 0, batchSize)
				namesBatch := make([][]byte, 0, batchSize)
				tagsBatch := make([]models.Tags, 0, batchSize)

				seriesPerRound := 2000
				baseIdx := (batch*1000 + round*seriesPerRound) + 100000

				for i := baseIdx; i < baseIdx+seriesPerRound; i++ {
					measurement := fmt.Sprintf("tsi_measurement_%d", i)
					seriesKey := fmt.Sprintf("%s,host=server%d,batch=%d,round=%d,region=r%d,dc=dc%d",
						measurement, i%50, batch, round, i%20, i%15)
					tags := models.ParseTags([]byte(fmt.Sprintf("host=server%d,batch=%d,round=%d,region=r%d,dc=dc%d",
						i%50, batch, round, i%20, i%15)))

					keysBatch = append(keysBatch, []byte(seriesKey))
					namesBatch = append(namesBatch, []byte(measurement))
					tagsBatch = append(tagsBatch, tags)

					if len(keysBatch) == batchSize {
						if err := tsiIndex.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch, tsdb.NoopStatsTracker()); err != nil {
							t.Fatal(err)
						}
						keysBatch = keysBatch[:0]
						namesBatch = namesBatch[:0]
						tagsBatch = tagsBatch[:0]
					}
				}

				if len(keysBatch) > 0 {
					if err := tsiIndex.CreateSeriesListIfNotExists(keysBatch, namesBatch, tagsBatch, tsdb.NoopStatsTracker()); err != nil {
						t.Fatal(err)
					}
				}

				tsiIndex.Compact()
				tsiIndex.Wait()
			}

			tsiIndex.Close()

			tempTSIFiles, _ := filepath.Glob(filepath.Join(tempPath, "*", "*.tsi"))

			for _, srcPath := range tempTSIFiles {
				level := "L1"
				if tsiFileNum%4 == 1 {
					level = "L2"
				} else if tsiFileNum%4 == 2 {
					level = "L3"
				} else if tsiFileNum%4 == 3 {
					level = "L4"
				}

				newName := fmt.Sprintf("%s-%08d.tsi", level, tsiFileNum)
				dstPath := filepath.Join(p.Path(), newName)

				if err := os.Rename(srcPath, dstPath); err != nil {
					fmt.Printf("Error renaming %s to %s: %v\n", srcPath, dstPath, err)
				} else {
					allFiles = append(allFiles, newName)
					tsiFileNum++
				}
			}

			os.RemoveAll(tempPath)

			if batch%10 == 0 {
				fmt.Printf("Completed batch %d, total TSI files so far: %d\n", batch, tsiFileNum)
			}
		}

		mpath := filepath.Join(p.Path(), tsi1.ManifestFileName)
		m := tsi1.NewManifest(mpath)
		m.Levels = nil
		m.Version = 1
		m.Files = allFiles

		if _, err := m.Write(); err != nil {
			t.Fatal(err)
		}

		// TODO: I need to probably close the partition and re-open it so
		// all the files are accounted for.
		fmt.Printf("FINAL RESULT: Created %d TSL files + %d TSI files = %d total files\n",
			100, len(allFiles)-100, len(allFiles))

		fmt.Println("Starting compaction!!!")
		err = p.Reopen()
		require.NoError(t, err, "Reopen failed")
		p.Compact()
		fmt.Println("Finished compaction!!!")

		err = p.Close()
		require.NoError(t, err, "partition close")
	})
}

// Partition is a test wrapper for tsi1.Partition.
type Partition struct {
	*tsi1.Partition
}

// NewPartition returns a new instance of Partition at a temporary path.
func NewPartition(sfile *tsdb.SeriesFile) *Partition {
	return &Partition{Partition: tsi1.NewPartition(sfile, MustTempPartitionDir())}
}

// MustOpenPartition returns a new, open index. Panic on error.
func MustOpenPartition(sfile *tsdb.SeriesFile) *Partition {
	p := NewPartition(sfile)
	if err := p.Open(); err != nil {
		panic(err)
	}
	return p
}

// Close closes and removes the index directory.
func (p *Partition) Close() error {
	defer os.RemoveAll(p.Path())
	return p.Partition.Close()
}

// Reopen closes and opens the index.
func (p *Partition) Reopen() error {
	if err := p.Partition.Close(); err != nil {
		return err
	}

	sfile, path := p.SeriesFile(), p.Path()
	p.Partition = tsi1.NewPartition(sfile, path)
	return p.Open()
}
