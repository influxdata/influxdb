package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
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
name with '.%v'. The backups will be ignored by the system since they
are not registered with the cluster.

To restore a backup, delete the tsm1 version, rename the backup directory
restart the node.`, backupExt)

type options struct {
	DataPath       string
	DBs            []string
	TSMSize        uint64
	Parallel       bool
	SkipBackup     bool
	UpdateInterval time.Duration
	Quiet          bool
}

func (o *options) Parse() error {
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	var dbs string

	fs.StringVar(&dbs, "dbs", "", "Comma-delimited list of databases to convert. Default is to convert all databases.")
	fs.Uint64Var(&opts.TSMSize, "sz", maxTSMSz, "Maximum size of individual TSM files.")
	fs.BoolVar(&opts.Parallel, "parallel", false, "Perform parallel conversion. (up to GOMAXPROCS shards at once)")
	fs.BoolVar(&opts.SkipBackup, "nobackup", false, "Disable database backups. Not recommended.")
	fs.BoolVar(&opts.Quiet, "quiet", false, "Suppresses the regular status updates.")
	fs.DurationVar(&opts.UpdateInterval, "interval", 5*time.Second, "How often status updates are printed.")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %v [options] <data-path> \n", os.Args[0])
		fmt.Fprintf(os.Stderr, "%v\n\n", description)
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\n")
	}

	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	if len(fs.Args()) < 1 {
		return errors.New("no data directory specified")
	}
	o.DataPath = fs.Args()[0]

	if o.TSMSize > maxTSMSz {
		return fmt.Errorf("bad TSM file size, maximum TSM file size is %d", maxTSMSz)
	}

	// Check if specific databases were requested.
	o.DBs = strings.Split(dbs, ",")
	if len(o.DBs) == 1 && o.DBs[0] == "" {
		o.DBs = nil
	}

	return nil
}

var opts options

const maxTSMSz = 2 * 1000 * 1000 * 1000

func init() {
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
}

func main() {
	if err := opts.Parse(); err != nil {
		log.Fatal(err)
	}

	// Determine the list of databases
	dbs, err := ioutil.ReadDir(opts.DataPath)
	if err != nil {
		log.Fatalf("failed to access data directory at %v: %v\n", opts.DataPath, err)
	}
	fmt.Println() // Cleanly separate output from start of program.

	if opts.Parallel {
		if !isEnvSet("GOMAXPROCS") {
			// Only modify GOMAXPROCS if it wasn't set in the environment
			// This means 'GOMAXPROCS=1 influx_tsm -parallel' will not actually
			// run in parallel
			runtime.GOMAXPROCS(runtime.NumCPU())
		}
	}

	// Dump summary of what is about to happen.
	fmt.Println("b1 and bz1 shard conversion.")
	fmt.Println("-----------------------------------")
	fmt.Println("Data directory is:       ", opts.DataPath)
	fmt.Println("Databases specified:     ", allDBs(opts.DBs))
	fmt.Println("Database backups enabled:", yesno(!opts.SkipBackup))
	fmt.Println("Parallel mode enabled:   ", yesno(opts.Parallel), runtime.GOMAXPROCS(0))
	fmt.Println()

	shards := collectShards(dbs)

	// Anything to convert?
	fmt.Printf("\nFound %d shards that will be converted.\n", len(shards))
	if len(shards) == 0 {
		fmt.Println("Nothing to do.")
		return
	}

	// Display list of convertible shards.
	fmt.Println()
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 1, '\t', 0)
	fmt.Fprintln(w, "Database\tRetention\tPath\tEngine\tSize")
	for _, si := range shards {
		fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%d\n", si.Database, si.RetentionPolicy, si.FullPath(opts.DataPath), si.FormatAsString(), si.Size)
	}
	w.Flush()

	// Get confirmation from user.
	fmt.Printf("\nThese shards will be converted. Proceed? y/N: ")
	liner := bufio.NewReader(os.Stdin)
	yn, err := liner.ReadString('\n')
	if err != nil {
		log.Fatalf("failed to read response: %v", err)
	}
	yn = strings.TrimRight(strings.ToLower(yn), "\n")
	if yn != "y" {
		log.Fatal("Conversion aborted.")
	}
	fmt.Println("Conversion starting....")

	// GOMAXPROCS(0) just queires the current value
	pg := NewParallelGroup(runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup

	conversionStart := time.Now()

	// Backup each directory.
	if !opts.SkipBackup {
		databases := shards.Databases()
		fmt.Printf("Backing up %d databases...\n", len(databases))
		wg.Add(len(databases))
		for i := range databases {
			db := databases[i]
			go pg.Do(func() {
				defer wg.Done()

				start := time.Now()
				log.Printf("Backup of databse '%v' started", db)
				err := backupDatabase(filepath.Join(opts.DataPath, db))
				if err != nil {
					log.Fatalf("Backup of database %v failed: %v\n", db, err)
				}
				log.Printf("Database %v backed up (%v)\n", db, time.Now().Sub(start))
			})
		}
		wg.Wait()
	} else {
		fmt.Println("Database backup disabled.")
	}

	wg.Add(len(shards))
	for i := range shards {
		si := shards[i]
		go pg.Do(func() {
			defer wg.Done()

			start := time.Now()
			log.Printf("Starting conversion of shard: %v", si.FullPath(opts.DataPath))
			if err := convertShard(si); err != nil {
				log.Fatalf("Failed to convert %v: %v\n", si.FullPath(opts.DataPath), err)
			}
			log.Printf("Conversion of %v successful (%v)\n", si.FullPath(opts.DataPath), time.Since(start))
		})
	}
	wg.Wait()

	// Dump stats.
	preSize := shards.Size()
	postSize := TsmBytesWritten
	totalTime := time.Since(conversionStart)

	fmt.Printf("\nSummary statistics\n========================================\n")
	fmt.Printf("Databases converted:                 %d\n", len(shards.Databases()))
	fmt.Printf("Shards converted:                    %d\n", len(shards))
	fmt.Printf("TSM files created:                   %d\n", TsmFilesCreated)
	fmt.Printf("Points read:                         %d\n", PointsRead)
	fmt.Printf("Points written:                      %d\n", PointsWritten)
	fmt.Printf("NaN filtered:                        %d\n", NanFiltered)
	fmt.Printf("Inf filtered:                        %d\n", InfFiltered)
	fmt.Printf("Points without fields filtered:      %d\n", b1.NoFieldsFiltered+bz1.NoFieldsFiltered)
	fmt.Printf("Disk usage pre-conversion (bytes):   %d\n", preSize)
	fmt.Printf("Disk usage post-conversion (bytes):  %d\n", postSize)
	fmt.Printf("Reduction factor:                    %d%%\n", 100*(preSize-postSize)/preSize)
	fmt.Printf("Bytes per TSM point:                 %.2f\n", float64(postSize)/float64(PointsWritten))
	fmt.Printf("Total conversion time:               %v\n", totalTime)
	fmt.Println()
}

func collectShards(dbs []os.FileInfo) tsdb.ShardInfos {
	// Get the list of shards for conversion.
	var shards tsdb.ShardInfos
	for _, db := range dbs {
		if strings.HasSuffix(db.Name(), backupExt) {
			log.Printf("Skipping %v as it looks like a backup.\n", db.Name())
			continue
		}

		d := tsdb.NewDatabase(filepath.Join(opts.DataPath, db.Name()))
		shs, err := d.Shards()
		if err != nil {
			log.Fatalf("Failed to access shards for database %v: %v\n", d.Name(), err)
		}
		shards = append(shards, shs...)
	}

	sort.Sort(shards)
	shards = shards.FilterFormat(tsdb.TSM1)
	if len(dbs) > 0 {
		shards = shards.ExclusiveDatabases(opts.DBs)
	}

	return shards
}

// backupDatabase backs up the database at src.
func backupDatabase(src string) error {
	dest := filepath.Join(src + "." + backupExt)
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
	src := si.FullPath(opts.DataPath)
	dst := fmt.Sprintf("%v.%v", src, tsmExt)

	var reader ShardReader
	switch si.Format {
	case tsdb.BZ1:
		reader = bz1.NewReader(src)
	case tsdb.B1:
		reader = b1.NewReader(src)
	default:
		return fmt.Errorf("Unsupported shard format: %v", si.FormatAsString())
	}
	defer reader.Close()

	// Open the shard, and create a converter.
	if err := reader.Open(); err != nil {
		return fmt.Errorf("Failed to open %v for conversion: %v", src, err)
	}
	converter := NewConverter(dst, uint32(opts.TSMSize))

	// Perform the conversion.
	if err := converter.Process(reader); err != nil {
		return fmt.Errorf("Conversion of %v failed: %v", src, err)
	}

	// Delete source shard, and rename new tsm1 shard.
	if err := reader.Close(); err != nil {
		return fmt.Errorf("Conversion of %v failed due to close: %v", src, err)
	}

	if err := os.RemoveAll(si.FullPath(opts.DataPath)); err != nil {
		return fmt.Errorf("Deletion of %v failed: %v", src, err)
	}
	if err := os.Rename(dst, src); err != nil {
		return fmt.Errorf("Rename of %v to %v failed: %v", dst, src, err)
	}

	return nil
}

// ParallelGroup allows the maximum parrallelism of a set of operations to be controlled.
type ParallelGroup chan struct{}

// NewParallelGroup returns a group which allows n operations to run in parallel. A value of 0
// means no operations will ever run.
func NewParallelGroup(n int) ParallelGroup {
	return make(chan struct{}, n)
}

func (p ParallelGroup) Do(f func()) {
	p <- struct{}{} // acquire working slot
	defer func() { <-p }()

	f()
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

// isEnvSet checks to see if a variable was set in the environment
func isEnvSet(name string) bool {
	for _, s := range os.Environ() {
		if strings.SplitN(s, "=", 2)[0] == name {
			return true
		}
	}
	return false
}
