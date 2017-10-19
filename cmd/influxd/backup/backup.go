// Package backup implements both the backup and export subcommands for the influxd command.
package backup

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/services/snapshotter"
	"github.com/influxdata/influxdb/tcp"
)

const (
	// Suffix is a suffix added to the backup while it's in-process.
	Suffix = ".pending"

	// Metafile is the base name given to the metastore backups.
	Metafile = "meta"

	// BackupFilePattern is the beginning of the pattern for a backup
	// file. They follow the scheme <database>.<retention>.<shardID>.<increment>
	BackupFilePattern = "%s.%s.%05d"
)

// Command represents the program execution for "influxd backup".
type Command struct {
	// The logger passed to the ticker during execution.
	StdoutLogger *log.Logger
	StderrLogger *log.Logger

	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	host            string
	path            string
	database        string
	retentionPolicy string
	shardID         string

	isBackup bool
	since    time.Time
	start    time.Time
	end      time.Time
}

// NewCommand returns a new instance of Command with default settings.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

// Run executes the program.
func (cmd *Command) Run(args ...string) error {
	// Set up logger.
	cmd.StdoutLogger = log.New(cmd.Stdout, "", log.LstdFlags)
	cmd.StderrLogger = log.New(cmd.Stderr, "", log.LstdFlags)

	// Parse command line arguments.
	err := cmd.parseFlags(args)
	if err != nil {
		return err
	}

	if cmd.shardID != "" {
		// always backup the metastore
		if err := cmd.extractMetastore(cmd.database); err != nil {
			return err
		}
		err = cmd.extractShard(cmd.retentionPolicy, cmd.shardID)

	} else if cmd.retentionPolicy != "" {
		// always backup the metastore
		if err := cmd.extractMetastore(cmd.database); err != nil {
			return err
		}
		err = cmd.extractRetentionPolicy()
	} else if cmd.database != "" {
		// always backup the metastore
		if err := cmd.extractMetastore(cmd.database); err != nil {
			return err
		}
		err = cmd.extractDatabase()
	} else {
		// always backup the metastore
		if err := cmd.extractMetastore(""); err != nil {
			return err
		}
		cmd.StdoutLogger.Printf("No database, retention policy or shard ID given. Full meta store backed up.")
	}

	if err != nil {
		cmd.StderrLogger.Printf("backup failed: %v", err)
		return err
	}
	cmd.StdoutLogger.Printf("backup complete")

	return nil
}

// parseFlags parses and validates the command line arguments into a request object.
func (cmd *Command) parseFlags(args []string) (err error) {
	fs := flag.NewFlagSet("", flag.ContinueOnError)

	fs.StringVar(&cmd.host, "host", "localhost:8088", "")
	fs.StringVar(&cmd.database, "database", "", "")
	fs.StringVar(&cmd.retentionPolicy, "retention", "", "")
	fs.StringVar(&cmd.shardID, "shard", "", "")
	var sinceArg string
	var startArg string
	var endArg string
	fs.StringVar(&sinceArg, "since", "", "")
	fs.StringVar(&startArg, "start", "", "")
	fs.StringVar(&endArg, "end", "", "")

	fs.SetOutput(cmd.Stderr)
	fs.Usage = cmd.printUsage

	err = fs.Parse(args)
	if err != nil {
		return err
	}

	// if startArg and endArg are unspecified, then assume we are doing a full backup of the DB
	cmd.isBackup = startArg == "" && endArg == ""

	if sinceArg != "" {
		cmd.since, err = time.Parse(time.RFC3339, sinceArg)
		if err != nil {
			return err
		}
	}
	if startArg != "" {
		if cmd.isBackup {
			return errors.New("backup command uses one of -since or -start/-end")
		}
		cmd.start, err = time.Parse(time.RFC3339, startArg)
		if err != nil {
			return err
		}
	}

	if endArg != "" {
		if cmd.isBackup {
			return errors.New("backup command uses one of -since or -start/-end")
		}
		cmd.end, err = time.Parse(time.RFC3339, endArg)
		if err != nil {
			return err
		}
	}

	// some validations
	// 1.  -database is required
	if cmd.database == "" {
		return errors.New("-database <dbname> is a required argument")
	}

	// 2.  start should be < end
	if cmd.start.After(cmd.end) {
		return errors.New("start date must be before end date")
	}

	// Ensure that only one arg is specified.
	if fs.NArg() != 1 {
		return errors.New("Exactly one backup path is required.")
	}
	cmd.path = fs.Arg(0)

	err = os.MkdirAll(cmd.path, 0700)

	return err
}

func (cmd *Command) extractShard(rp, sid string) error {
	if cmd.isBackup {
		return cmd.backupShard(rp, sid)
	}
	return cmd.exportShard(rp, sid)
}

// backupShard will write a tar archive of the passed in shard with any TSM files that have been
// created since the time passed in
func (cmd *Command) backupShard(rp, sid string) error {
	id, err := strconv.ParseUint(sid, 10, 64)
	if err != nil {
		return err
	}

	shardArchivePath, err := cmd.nextPath(filepath.Join(cmd.path, fmt.Sprintf(BackupFilePattern, cmd.database, rp, id)))
	if err != nil {
		return err
	}

	cmd.StdoutLogger.Printf("backing up db=%v rp=%v shard=%v to %s since %s",
		cmd.database, rp, sid, shardArchivePath, cmd.since)

	req := &snapshotter.Request{
		Type:            snapshotter.RequestShardBackup,
		Database:        cmd.database,
		RetentionPolicy: rp,
		ShardID:         id,
		Since:           cmd.since,
	}

	// TODO: verify shard backup data
	return cmd.downloadAndVerify(req, shardArchivePath, nil)
}

// exportShard will write a gzip archive of the passed in shard with any TSM files that have been
// created since the time passed in
func (cmd *Command) exportShard(rp, sid string) error {
	id, err := strconv.ParseUint(sid, 10, 64)
	if err != nil {
		return err
	}

	shardArchivePath, err := cmd.nextPath(filepath.Join(cmd.path, fmt.Sprintf(BackupFilePattern, cmd.database, rp, id)))
	if err != nil {
		return err
	}

	cmd.StdoutLogger.Printf("exporting db=%v rp=%v shard=%v to %s start %s end  %s",
		cmd.database, rp, sid, shardArchivePath, cmd.start, cmd.end)

	req := &snapshotter.Request{
		Type:            snapshotter.RequestShardExport,
		Database:        cmd.database,
		RetentionPolicy: rp,
		ShardID:         id,
		ExportStart:     cmd.start,
		ExportEnd:       cmd.end,
	}

	// TODO: verify shard backup data
	return cmd.downloadAndVerify(req, shardArchivePath, nil)
}

// extractDatabase will request the database information from the server and then extract
// every shard in every retention policy in the database. Each shard will be written to a separate file.
func (cmd *Command) extractDatabase() error {
	cmd.StdoutLogger.Printf("extracting db=%s", cmd.database)

	req := &snapshotter.Request{
		Type:     snapshotter.RequestDatabaseInfo,
		Database: cmd.database,
	}

	response, err := cmd.requestInfo(req)
	if err != nil {
		return err
	}

	return cmd.extractResponsePaths(response)
}

// extractRetentionPolicy will request the retention policy information from the server and then extract
// every shard in the retention policy. Each shard will be written to a separate file.
func (cmd *Command) extractRetentionPolicy() error {
	cmd.StdoutLogger.Printf("backing up rp=%s since %s", cmd.retentionPolicy, cmd.since)

	req := &snapshotter.Request{
		Type:            snapshotter.RequestRetentionPolicyInfo,
		Database:        cmd.database,
		RetentionPolicy: cmd.retentionPolicy,
	}

	response, err := cmd.requestInfo(req)
	if err != nil {
		return err
	}

	return cmd.extractResponsePaths(response)
}

// extractResponsePaths will extract all shards identified by shard paths in the response struct
func (cmd *Command) extractResponsePaths(response *snapshotter.Response) error {

	// loop through the returned paths and back up each shard
	for _, path := range response.Paths {
		fmt.Printf("path: %s", path)
		rp, id, err := retentionAndShardFromPath(path)
		if err != nil {
			return err
		}

		err = cmd.extractShard(rp, id)

		if err != nil {
			return err
		}
	}

	return nil
}

// extractMetastore will backup the whole metastore on the host to the passed in path.
func (cmd *Command) extractMetastore(useDB string) error {
	metastoreArchivePath, err := cmd.nextPath(filepath.Join(cmd.path, Metafile))
	if err != nil {
		return err
	}

	cmd.StdoutLogger.Printf("backing up metastore to %s", metastoreArchivePath)

	req := &snapshotter.Request{
		Type:     snapshotter.RequestMetastoreBackup,
		Database: useDB,
	}

	return cmd.downloadAndVerify(req, metastoreArchivePath, func(file string) error {
		f, err := os.Open(file)
		if err != nil {
			return err
		}

		magicByte := make([]byte, 8)
		n, err := f.Read(magicByte)
		if err != nil {
			return err
		}

		if n < 8 {
			return errors.New("Not enough bytes data to verify")
		}

		magic := binary.BigEndian.Uint64(magicByte)
		if magic != snapshotter.BackupMagicHeader {
			cmd.StderrLogger.Println("Invalid metadata blob, ensure the metadata service is running (default port 8088)")
			return errors.New("invalid metadata received")
		}

		return nil
	})
}

// nextPath returns the next file to write to.
func (cmd *Command) nextPath(path string) (string, error) {
	// Iterate through incremental files until one is available.
	for i := 0; ; i++ {
		s := fmt.Sprintf(path+".%02d", i)
		if _, err := os.Stat(s); os.IsNotExist(err) {
			return s, nil
		} else if err != nil {
			return "", err
		}
	}
}

// downloadAndVerify will download either the metastore or shard to a temp file and then
// rename it to a good backup file name after complete
func (cmd *Command) downloadAndVerify(req *snapshotter.Request, path string, validator func(string) error) error {
	tmppath := path + Suffix
	if err := cmd.download(req, tmppath); err != nil {
		return err
	}

	if validator != nil {
		if err := validator(tmppath); err != nil {
			if rmErr := os.Remove(tmppath); rmErr != nil {
				cmd.StderrLogger.Printf("Error cleaning up temporary file: %v", rmErr)
			}
			return err
		}
	}

	f, err := os.Stat(tmppath)
	if err != nil {
		return err
	}

	// There was nothing downloaded, don't create an empty backup file.
	if f.Size() == 0 {
		return os.Remove(tmppath)
	}

	// Rename temporary file to final path.
	if err := os.Rename(tmppath, path); err != nil {
		return fmt.Errorf("rename: %s", err)
	}

	return nil
}

// download downloads a snapshot of either the metastore or a shard from a host to a given path.
func (cmd *Command) download(req *snapshotter.Request, path string) error {
	// Create local file to write to.
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("open temp file: %s", err)
	}
	defer f.Close()

	for i := 0; i < 10; i++ {
		if err = func() error {
			// Connect to snapshotter service.
			conn, err := tcp.Dial("tcp", cmd.host, snapshotter.MuxHeader)
			if err != nil {
				return err
			}
			defer conn.Close()

			// Write the request
			if err := json.NewEncoder(conn).Encode(req); err != nil {
				return fmt.Errorf("encode snapshot request: %s", err)
			}

			// Read snapshot from the connection
			if n, err := io.Copy(f, conn); err != nil || n == 0 {
				return fmt.Errorf("copy backup to file: err=%v, n=%d", err, n)
			}
			return nil
		}(); err == nil {
			break
		} else if err != nil {
			cmd.StderrLogger.Printf("Download shard %v failed %s.  Retrying (%d)...\n", req.ShardID, err, i)
			time.Sleep(time.Second)
		}
	}

	return err
}

// requestInfo will request the database or retention policy information from the host
func (cmd *Command) requestInfo(request *snapshotter.Request) (*snapshotter.Response, error) {
	// Connect to snapshotter service.
	conn, err := tcp.Dial("tcp", cmd.host, snapshotter.MuxHeader)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Write the request
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return nil, fmt.Errorf("encode snapshot request: %s", err)
	}

	// Read the response
	var r snapshotter.Response
	if err := json.NewDecoder(conn).Decode(&r); err != nil {
		return nil, err
	}

	return &r, nil
}

// printUsage prints the usage message to STDERR.
func (cmd *Command) printUsage() {

	fmt.Fprintf(cmd.Stdout, `Downloads a file level age-based snapshot of a data node and saves it to disk.

Usage: influxd backup [flags] PATH

    -host <host:port>
            The host to connect to snapshot. Defaults to 127.0.0.1:8088.
    -database <name>
            The database to backup.
    -retention <name>
            Optional. The retention policy to backup.
    -shard <id>
            Optional. The shard id to backup. If specified, retention is required.
    -since <2015-12-24T08:12:23Z>
            Optional. Do an incremental backup since the passed in RFC3339
            formatted time.  Not compatible with -start or -end.
	-start <2015-12-24T08:12:23Z>
            All points earlier than this time stamp will be excluded from the export. Not compatible with -since.
	-end <2015-12-24T08:12:23Z>
            All points later than this time stamp will be excluded from the export. Not compatible with -since.

`)

}

// retentionAndShardFromPath will take the shard relative path and split it into the
// retention policy name and shard ID. The first part of the path should be the database name.
func retentionAndShardFromPath(path string) (retention, shard string, err error) {
	a := strings.Split(path, string(filepath.Separator))
	if len(a) != 3 {
		return "", "", fmt.Errorf("expected database, retention policy, and shard id in path: %s", path)
	}

	return a[1], a[2], nil
}
