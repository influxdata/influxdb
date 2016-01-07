package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/influxdb/influxdb/cmd/influx_tsm/b1"
	"github.com/influxdb/influxdb/cmd/influx_tsm/bz1"
	"github.com/influxdb/influxdb/cmd/influx_tsm/tsdb"
)

type ShardReader interface {
	KeyIterator
	Open() error
	Close() error
}

const (
	backupExt = "bak"
	tsmExt    = "tsm"
)

var description = fmt.Sprintf(`
Convert a database from b1 or bz1 format to tsm1 format.

This tool will backup any directory before conversion. It is up to the
end-user to delete the backup on the disk, once the end-user is happy
with the converted data. Backups are named by suffixing the database
name with '.%s'. The backups will be ignored by the system since they
are not registered with the cluster.

To restore a backup, delete the tsm1 version, rename the backup directory
restart the node.`, backupExt)

var dataPath string
var ds string
var tsmSz uint64
var parallel bool
var disBack bool

const maxTSMSz = 2 * 1000 * 1000 * 1000

func init() {
	flag.StringVar(&ds, "dbs", "", "Comma-delimited list of databases to convert. Default is to convert all databases.")
	flag.Uint64Var(&tsmSz, "sz", maxTSMSz, "Maximum size of individual TSM files.")
	flag.BoolVar(&parallel, "parallel", false, "Perform parallel conversion.")
	flag.BoolVar(&disBack, "nobackup", false, "Disable database backups. Not recommended.")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <data-path> \n", os.Args[0])
		fmt.Fprintf(os.Stderr, "%s\n\n", description)
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\n")
	}
}

func main() {
	pg := NewParallelGroup(1)

	flag.Parse()
	if len(flag.Args()) < 1 {
		fmt.Fprintf(os.Stderr, "No data directory specified\n")
		os.Exit(1)
	}
	dataPath = flag.Args()[0]

	if tsmSz > maxTSMSz {
		fmt.Fprintf(os.Stderr, "Maximum TSM file size is %d\n", maxTSMSz)
		os.Exit(1)
	}

	// Check if specific directories were requested.
	reqDs := strings.Split(ds, ",")
	if len(reqDs) == 1 && reqDs[0] == "" {
		reqDs = nil
	}

	// Determine the list of databases
	dbs, err := ioutil.ReadDir(dataPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to access data directory at %s: %s\n", dataPath, err.Error())
		os.Exit(1)
	}
	fmt.Println() // Cleanly separate output from start of program.

	// Dump summary of what is about to happen.
	fmt.Println("b1 and bz1 shard conversion.")
	fmt.Println("-----------------------------------")
	fmt.Println("Data directory is:       ", dataPath)
	fmt.Println("Databases specified:     ", allDBs(reqDs))
	fmt.Println("Database backups enabled:", yesno(!disBack))
	fmt.Println("Parallel mode enabled:   ", yesno(parallel))
	fmt.Println()

	// Get the list of shards for conversion.
	var shards []*tsdb.ShardInfo
	for _, db := range dbs {
		if strings.HasSuffix(db.Name(), backupExt) {
			fmt.Printf("Skipping %s as it looks like a backup.\n", db.Name())
			continue
		}

		d := tsdb.NewDatabase(filepath.Join(dataPath, db.Name()))
		shs, err := d.Shards()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to access shards for database %s: %s\n", d.Name(), err.Error())
			os.Exit(1)
		}
		shards = append(shards, shs...)
	}
	sort.Sort(tsdb.ShardInfos(shards))
	usl := len(shards)
	shards = tsdb.ShardInfos(shards).FilterFormat(tsdb.TSM1).ExclusiveDatabases(reqDs)
	sl := len(shards)

	// Anything to convert?
	fmt.Printf("\n%d shard(s) detected, %d non-TSM shards detected.\n", usl, sl)
	if len(shards) == 0 {
		fmt.Printf("Nothing to do.\n")
		os.Exit(0)
	}

	// Display list of convertible shards.
	fmt.Println()
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 1, '\t', 0)
	fmt.Fprintln(w, "Database\tRetention\tPath\tEngine\tSize")
	for _, si := range shards {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\n", si.Database, si.RetentionPolicy, si.FullPath(dataPath), si.FormatAsString(), si.Size)
	}
	w.Flush()

	// Get confirmation from user.
	fmt.Printf("\nThese shards will be converted. Proceed? y/N: ")
	liner := bufio.NewReader(os.Stdin)
	yn, err := liner.ReadString('\n')
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read response: %s", err.Error())
		os.Exit(1)
	}
	yn = strings.TrimRight(strings.ToLower(yn), "\n")
	if yn != "y" {
		fmt.Println("Conversion aborted.")
		os.Exit(1)
	}
	fmt.Println("Conversion starting....")

	// Backup each directory.
	conversionStart := time.Now()
	if !disBack {
		databases := tsdb.ShardInfos(shards).Databases()
		fmt.Printf("Backing up %d databases...\n", len(databases))
		if parallel {
			pg = NewParallelGroup(len(databases))
		}
		for _, db := range databases {
			pg.Request()
			go func(db string) {
				defer pg.Release()

				start := time.Now()
				err := backupDatabase(filepath.Join(dataPath, db))
				if err != nil {
					fmt.Fprintf(os.Stderr, "Backup of database %s failed: %s\n", db, err.Error())
					os.Exit(1)
				}
				fmt.Printf("Database %s backed up (%v)\n", db, time.Now().Sub(start))
			}(db)
		}
		pg.Wait()
	} else {
		fmt.Println("Database backup disabled.")
	}

	// Convert each shard.
	if parallel {
		pg = NewParallelGroup(len(shards))
	}
	for _, si := range shards {
		pg.Request()
		go func(si *tsdb.ShardInfo) {
			defer pg.Release()

			start := time.Now()
			if err := convertShard(si); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to convert %s: %s\n", si.FullPath(dataPath), err.Error())
				os.Exit(1)
			}
			fmt.Printf("Conversion of %s successful (%s)\n", si.FullPath(dataPath), time.Now().Sub(start))
		}(si)
	}
	pg.Wait()

	// Dump stats.
	preSize := tsdb.ShardInfos(shards).Size()
	postSize := TsmBytesWritten
	fmt.Printf("\nSummary statistics\n========================================\n")
	fmt.Printf("Databases converted:                 %d\n", len(tsdb.ShardInfos(shards).Databases()))
	fmt.Printf("Shards converted:                    %d\n", len(shards))
	fmt.Printf("TSM files created:                   %d\n", TsmFilesCreated)
	fmt.Printf("Points read:                         %d\n", PointsRead)
	fmt.Printf("Points written:                      %d\n", PointsWritten)
	fmt.Printf("NaN filtered:                        %d\n", NanFiltered)
	fmt.Printf("Inf filtered:                        %d\n", InfFiltered)
	fmt.Printf("Points without fields filtered:      %d\n", b1.NoFieldsFiltered+bz1.NoFieldsFiltered)
	fmt.Printf("Disk usage pre-conversion (bytes):   %d\n", preSize)
	fmt.Printf("Disk usage post-conversion (bytes):  %d\n", postSize)
	fmt.Printf("Reduction factor:                    %d%%\n", (100*preSize-postSize)/preSize)
	fmt.Printf("Bytes per TSM point:                 %.2f\n", float64(postSize)/float64(PointsWritten))
	fmt.Printf("Total conversion time:               %v\n", time.Now().Sub(conversionStart))
	fmt.Println()
}

// backupDatabase backs up the database at src.
func backupDatabase(src string) error {
	dest := filepath.Join(src + "." + backupExt)
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		return fmt.Errorf("backup of %s already exists", src)
	}
	return copyDir(dest, src)
}

// copyDir copies the directory at src to dest. If dest does not exist it
// will be created. It is up to the caller to ensure the paths don't overlap.
func copyDir(dest, src string) error {
	copyFile := func(path string, info os.FileInfo, err error) error {
		// Strip the src from the path and replace with dest.
		toPath := strings.Replace(path, src, dest, 1)

		// Copy it.
		if info.IsDir() {
			if err := os.MkdirAll(toPath, info.Mode()); err != nil {
				return err
			}
		} else {
			err := func() error {
				in, err := os.Open(path)
				if err != nil {
					return err
				}
				defer in.Close()

				out, err := os.OpenFile(toPath, os.O_CREATE|os.O_WRONLY, info.Mode())
				if err != nil {
					return err
				}
				defer out.Close()

				_, err = io.Copy(out, in)
				return err
			}()
			if err != nil {
				return err
			}
		}
		return nil
	}

	return filepath.Walk(src, copyFile)
}

// convertShard converts the shard in-place.
func convertShard(si *tsdb.ShardInfo) error {
	src := si.FullPath(dataPath)
	dst := fmt.Sprintf("%s.%s", src, tsmExt)

	var reader ShardReader
	switch si.Format {
	case tsdb.BZ1:
		reader = bz1.NewReader(src)
	case tsdb.B1:
		reader = b1.NewReader(src)
	default:
		return fmt.Errorf("Unsupported shard format: %s", si.FormatAsString())
	}
	defer reader.Close()

	// Open the shard, and create a converter.
	if err := reader.Open(); err != nil {
		return fmt.Errorf("Failed to open %s for conversion: %s", src, err.Error())
	}
	converter := NewConverter(dst, uint32(tsmSz))

	// Perform the conversion.
	if err := converter.Process(reader); err != nil {
		return fmt.Errorf("Conversion of %s failed: %s", src, err.Error())
	}

	// Delete source shard, and rename new tsm1 shard.
	if err := reader.Close(); err != nil {
		return fmt.Errorf("Conversion of %s failed due to close: %s", src, err.Error())
	}

	if err := os.RemoveAll(si.FullPath(dataPath)); err != nil {
		return fmt.Errorf("Deletion of %s failed: %s", src, err.Error())
	}
	if err := os.Rename(dst, src); err != nil {
		return fmt.Errorf("Rename of %s to %s failed: %s", dst, src, err.Error())
	}

	return nil
}

// ParallelGroup allows the maximum parrallelism of a set of operations to be controlled.
type ParallelGroup struct {
	c  chan struct{}
	wg sync.WaitGroup
}

// NewParallelGroup returns a group which allows n operations to run in parallel. A value of 0
// means no operations will ever run.
func NewParallelGroup(n int) *ParallelGroup {
	return &ParallelGroup{
		c: make(chan struct{}, n),
	}
}

// Request requests permission to start an operation. It will block unless and until
// the parallel requirements would not be violated.
func (p *ParallelGroup) Request() {
	p.wg.Add(1)
	p.c <- struct{}{}
}

// Release informs the group that a previoulsy requested operation has completed.
func (p *ParallelGroup) Release() {
	<-p.c
	p.wg.Done()
}

// Wait blocks until the ParallelGroup has no unreleased operations.
func (p *ParallelGroup) Wait() {
	p.wg.Wait()
}

// yesno returns "yes" for true, "no" for false.
func yesno(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// allDBs returns "all" if all databases are requested for conversion.
func allDBs(dbs []string) string {
	if dbs == nil {
		return "all"
	}
	return fmt.Sprintf("%v", dbs)
}
