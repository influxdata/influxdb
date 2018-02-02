// Package backup implements both the backup and export subcommands for the influxd command.
package backup

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/cmd/influxd/backup_util"
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

	portable         bool
	manifest         backup_util.Manifest
	portableFileBase string

	BackupFiles []string
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
		if err := cmd.backupMetastore(); err != nil {
			return err
		}
		err = cmd.backupShard(cmd.database, cmd.retentionPolicy, cmd.shardID)

	} else if cmd.retentionPolicy != "" {
		// always backup the metastore
		if err := cmd.backupMetastore(); err != nil {
			return err
		}
		err = cmd.backupRetentionPolicy()
	} else if cmd.database != "" {
		// always backup the metastore
		if err := cmd.backupMetastore(); err != nil {
			return err
		}
		err = cmd.backupDatabase()
	} else {
		// always backup the metastore
		if err := cmd.backupMetastore(); err != nil {
			return err
		}

		cmd.StdoutLogger.Println("No database, retention policy or shard ID given. Full meta store backed up.")
		if cmd.portable {
			cmd.StdoutLogger.Println("Backing up all databases in portable format")
			if err := cmd.backupDatabase(); err != nil {
				cmd.StderrLogger.Printf("backup failed: %v", err)
				return err
			}

		}

	}

	if cmd.portable {
		filename := cmd.portableFileBase + ".manifest"
		if err := cmd.manifest.Save(filepath.Join(cmd.path, filename)); err != nil {
			cmd.StderrLogger.Printf("manifest save failed: %v", err)
			return err
		}
		cmd.BackupFiles = append(cmd.BackupFiles, filename)
	}

	if err != nil {
		cmd.StderrLogger.Printf("backup failed: %v", err)
		return err
	}
	cmd.StdoutLogger.Println("backup complete:")
	for _, v := range cmd.BackupFiles {
		cmd.StdoutLogger.Println("\t" + filepath.Join(cmd.path, v))
	}

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
	fs.BoolVar(&cmd.portable, "portable", false, "")

	fs.SetOutput(cmd.Stderr)
	fs.Usage = cmd.printUsage

	err = fs.Parse(args)
	if err != nil {
		return err
	}

	cmd.BackupFiles = []string{}

	// for portable saving, if needed
	cmd.portableFileBase = time.Now().UTC().Format(backup_util.PortableFileNamePattern)

	// if startArg and endArg are unspecified, or if we are using -since then assume we are doing a full backup of the shards
	cmd.isBackup = (startArg == "" && endArg == "") || sinceArg != ""

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

		// start should be < end
		if !cmd.start.Before(cmd.end) {
			return errors.New("start date must be before end date")
		}
	}

	// Ensure that only one arg is specified.
	if fs.NArg() != 1 {
		return errors.New("Exactly one backup path is required.")
	}
	cmd.path = fs.Arg(0)

	err = os.MkdirAll(cmd.path, 0700)

	return err
}

func (cmd *Command) backupShard(db, rp, sid string) error {
	reqType := snapshotter.RequestShardBackup
	if !cmd.isBackup {
		reqType = snapshotter.RequestShardExport
	}

	id, err := strconv.ParseUint(sid, 10, 64)
	if err != nil {
		return err
	}

	shardArchivePath, err := cmd.nextPath(filepath.Join(cmd.path, fmt.Sprintf(backup_util.BackupFilePattern, db, rp, id)))
	if err != nil {
		return err
	}

	if cmd.isBackup {
		cmd.StdoutLogger.Printf("backing up db=%v rp=%v shard=%v to %s since %s",
			db, rp, sid, shardArchivePath, cmd.since.Format(time.RFC3339))
	} else {
		cmd.StdoutLogger.Printf("backing up db=%v rp=%v shard=%v to %s with boundaries start=%s, end=%s",
			db, rp, sid, shardArchivePath, cmd.start.Format(time.RFC3339), cmd.end.Format(time.RFC3339))
	}
	req := &snapshotter.Request{
		Type:                  reqType,
		BackupDatabase:        db,
		BackupRetentionPolicy: rp,
		ShardID:               id,
		Since:                 cmd.since,
		ExportStart:           cmd.start,
		ExportEnd:             cmd.end,
	}

	// TODO: verify shard backup data
	err = cmd.downloadAndVerify(req, shardArchivePath, nil)
	if !cmd.portable {
		cmd.BackupFiles = append(cmd.BackupFiles, shardArchivePath)
	}

	if err != nil {
		return err
	}

	if cmd.portable {
		f, err := os.Open(shardArchivePath)
		if err != nil {
			return err
		}
		defer f.Close()
		defer os.Remove(shardArchivePath)

		filePrefix := cmd.portableFileBase + ".s" + sid
		filename := filePrefix + ".tar.gz"
		out, err := os.OpenFile(filepath.Join(cmd.path, filename), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return err
		}

		zw := gzip.NewWriter(out)
		zw.Name = filePrefix + ".tar"

		cw := backup_util.CountingWriter{Writer: zw}

		_, err = io.Copy(&cw, f)
		if err != nil {
			if err := zw.Close(); err != nil {
				return err
			}

			if err := out.Close(); err != nil {
				return err
			}
			return err
		}

		shardid, err := strconv.ParseUint(sid, 10, 64)
		if err != nil {
			if err := zw.Close(); err != nil {
				return err
			}

			if err := out.Close(); err != nil {
				return err
			}
			return err
		}
		cmd.manifest.Files = append(cmd.manifest.Files, backup_util.Entry{
			Database:     db,
			Policy:       rp,
			ShardID:      shardid,
			FileName:     filename,
			Size:         cw.Total,
			LastModified: 0,
		})

		if err := zw.Close(); err != nil {
			return err
		}

		if err := out.Close(); err != nil {
			return err
		}

		cmd.BackupFiles = append(cmd.BackupFiles, filename)
	}
	return nil

}

// backupDatabase will request the database information from the server and then backup
// every shard in every retention policy in the database. Each shard will be written to a separate file.
func (cmd *Command) backupDatabase() error {
	cmd.StdoutLogger.Printf("backing up db=%s", cmd.database)

	req := &snapshotter.Request{
		Type:           snapshotter.RequestDatabaseInfo,
		BackupDatabase: cmd.database,
	}

	response, err := cmd.requestInfo(req)
	if err != nil {
		return err
	}

	return cmd.backupResponsePaths(response)
}

// backupRetentionPolicy will request the retention policy information from the server and then backup
// every shard in the retention policy. Each shard will be written to a separate file.
func (cmd *Command) backupRetentionPolicy() error {
	if cmd.isBackup {
		cmd.StdoutLogger.Printf("backing up rp=%s since %s", cmd.retentionPolicy, cmd.since.Format(time.RFC3339))
	} else {
		cmd.StdoutLogger.Printf("backing up rp=%s with boundaries start=%s, end=%s",
			cmd.retentionPolicy, cmd.start.Format(time.RFC3339), cmd.end.Format(time.RFC3339))
	}

	req := &snapshotter.Request{
		Type:                  snapshotter.RequestRetentionPolicyInfo,
		BackupDatabase:        cmd.database,
		BackupRetentionPolicy: cmd.retentionPolicy,
	}

	response, err := cmd.requestInfo(req)
	if err != nil {
		return err
	}

	return cmd.backupResponsePaths(response)
}

// backupResponsePaths will backup all shards identified by shard paths in the response struct
func (cmd *Command) backupResponsePaths(response *snapshotter.Response) error {

	// loop through the returned paths and back up each shard
	for _, path := range response.Paths {
		db, rp, id, err := backup_util.DBRetentionAndShardFromPath(path)
		if err != nil {
			return err
		}

		err = cmd.backupShard(db, rp, id)

		if err != nil {
			return err
		}
	}

	return nil
}

// backupMetastore will backup the whole metastore on the host to the backup path
// if useDB is non-empty, it will backup metadata only for the named database.
func (cmd *Command) backupMetastore() error {
	metastoreArchivePath, err := cmd.nextPath(filepath.Join(cmd.path, backup_util.Metafile))
	if err != nil {
		return err
	}

	cmd.StdoutLogger.Printf("backing up metastore to %s", metastoreArchivePath)

	req := &snapshotter.Request{
		Type: snapshotter.RequestMetastoreBackup,
	}

	err = cmd.downloadAndVerify(req, metastoreArchivePath, func(file string) error {
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		defer f.Close()

		var magicByte [8]byte
		n, err := io.ReadFull(f, magicByte[:])
		if err != nil {
			return err
		}

		if n < 8 {
			return errors.New("Not enough bytes data to verify")
		}

		magic := binary.BigEndian.Uint64(magicByte[:])
		if magic != snapshotter.BackupMagicHeader {
			cmd.StderrLogger.Println("Invalid metadata blob, ensure the metadata service is running (default port 8088)")
			return errors.New("invalid metadata received")
		}

		return nil
	})

	if err != nil {
		return err
	}

	if !cmd.portable {
		cmd.BackupFiles = append(cmd.BackupFiles, metastoreArchivePath)
	}

	if cmd.portable {
		metaBytes, err := backup_util.GetMetaBytes(metastoreArchivePath)
		defer os.Remove(metastoreArchivePath)
		if err != nil {
			return err
		}
		filename := cmd.portableFileBase + ".meta"
		ep := backup_util.PortablePacker{Data: metaBytes, MaxNodeID: 0}
		protoBytes, err := ep.MarshalBinary()
		if err != nil {
			return err
		}
		if err := ioutil.WriteFile(filepath.Join(cmd.path, filename), protoBytes, 0644); err != nil {
			fmt.Fprintln(cmd.Stdout, "Error.")
			return err
		}

		cmd.manifest.Meta.FileName = filename
		cmd.manifest.Meta.Size = int64(len(metaBytes))
		cmd.BackupFiles = append(cmd.BackupFiles, filename)
	}

	return nil
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
	tmppath := path + backup_util.Suffix
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

			_, err = conn.Write([]byte{byte(req.Type)})
			if err != nil {
				return err
			}

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
	var r snapshotter.Response
	conn, err := tcp.Dial("tcp", cmd.host, snapshotter.MuxHeader)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_, err = conn.Write([]byte{byte(request.Type)})
	if err != nil {
		return &r, err
	}

	// Write the request
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return nil, fmt.Errorf("encode snapshot request: %s", err)
	}

	// Read the response

	if err := json.NewDecoder(conn).Decode(&r); err != nil {
		return nil, err
	}

	return &r, nil
}

// printUsage prints the usage message to STDERR.
func (cmd *Command) printUsage() {
	fmt.Fprintf(cmd.Stdout, `
Downloads a file level age-based snapshot of a data node and saves it to disk.

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
    -portable
            Generate backup files in a format that is portable between different influxdb products.

`)

}
