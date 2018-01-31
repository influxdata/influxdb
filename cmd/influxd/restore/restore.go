// Package restore is the restore subcommand for the influxd command,
// for restoring from a backup.
package restore

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"compress/gzip"

	"github.com/influxdata/influxdb/cmd/influxd/backup_util"
	tarstream "github.com/influxdata/influxdb/pkg/tar"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/snapshotter"
)

// Command represents the program execution for "influxd restore".
type Command struct {
	// The logger passed to the ticker during execution.
	StdoutLogger *log.Logger
	StderrLogger *log.Logger

	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	host   string
	client *snapshotter.Client

	backupFilesPath     string
	metadir             string
	datadir             string
	destinationDatabase string
	sourceDatabase      string
	backupRetention     string
	restoreRetention    string
	shard               uint64
	portable            bool
	online              bool
	manifestMeta        *backup_util.MetaEntry
	manifestFiles       map[uint64]*backup_util.Entry

	// TODO: when the new meta stuff is done this should not be exported or be gone
	MetaConfig *meta.Config

	shardIDMap map[uint64]uint64
}

// NewCommand returns a new instance of Command with default settings.
func NewCommand() *Command {
	return &Command{
		Stdout:     os.Stdout,
		Stderr:     os.Stderr,
		MetaConfig: meta.NewConfig(),
	}
}

// Run executes the program.
func (cmd *Command) Run(args ...string) error {
	// Set up logger.
	cmd.StdoutLogger = log.New(cmd.Stdout, "", log.LstdFlags)
	cmd.StderrLogger = log.New(cmd.Stderr, "", log.LstdFlags)
	if err := cmd.parseFlags(args); err != nil {
		return err
	}

	if cmd.portable {
		return cmd.runOnlinePortable()
	} else if cmd.online {
		return cmd.runOnlineLegacy()
	} else {
		return cmd.runOffline()
	}
}

func (cmd *Command) runOffline() error {
	if cmd.metadir != "" {
		if err := cmd.unpackMeta(); err != nil {
			return err
		}
	}

	if cmd.shard != 0 {
		return cmd.unpackShard(cmd.shard)
	} else if cmd.restoreRetention != "" {
		return cmd.unpackRetention()
	} else if cmd.datadir != "" {
		return cmd.unpackDatabase()
	}
	return nil
}

func (cmd *Command) runOnlinePortable() error {
	err := cmd.updateMetaPortable()
	if err != nil {
		cmd.StderrLogger.Printf("error updating meta: %v", err)
		return err
	}
	err = cmd.uploadShardsPortable()
	if err != nil {
		cmd.StderrLogger.Printf("error updating shards: %v", err)
		return err
	}
	return nil
}

func (cmd *Command) runOnlineLegacy() error {
	err := cmd.updateMetaLegacy()
	if err != nil {
		cmd.StderrLogger.Printf("error updating meta: %v", err)
		return err
	}
	err = cmd.uploadShardsLegacy()
	if err != nil {
		cmd.StderrLogger.Printf("error updating shards: %v", err)
		return err
	}
	return nil
}

// parseFlags parses and validates the command line arguments.
func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.StringVar(&cmd.host, "host", "localhost:8088", "")
	fs.StringVar(&cmd.metadir, "metadir", "", "")
	fs.StringVar(&cmd.datadir, "datadir", "", "")
	fs.StringVar(&cmd.destinationDatabase, "database", "", "")
	fs.StringVar(&cmd.restoreRetention, "retention", "", "")
	fs.StringVar(&cmd.sourceDatabase, "db", "", "")
	fs.StringVar(&cmd.destinationDatabase, "newdb", "", "")
	fs.StringVar(&cmd.backupRetention, "rp", "", "")
	fs.StringVar(&cmd.restoreRetention, "newrp", "", "")
	fs.Uint64Var(&cmd.shard, "shard", 0, "")
	fs.BoolVar(&cmd.online, "online", false, "")
	fs.BoolVar(&cmd.portable, "portable", false, "")
	fs.SetOutput(cmd.Stdout)
	fs.Usage = cmd.printUsage
	if err := fs.Parse(args); err != nil {
		return err
	}

	cmd.MetaConfig = meta.NewConfig()
	cmd.MetaConfig.Dir = cmd.metadir
	cmd.client = snapshotter.NewClient(cmd.host)

	// Require output path.
	cmd.backupFilesPath = fs.Arg(0)
	if cmd.backupFilesPath == "" {
		return fmt.Errorf("path with backup files required")
	}

	fi, err := os.Stat(cmd.backupFilesPath)
	if err != nil || !fi.IsDir() {
		return fmt.Errorf("backup path should be a valid directory: %s", cmd.backupFilesPath)
	}

	if cmd.portable || cmd.online {
		// validate the arguments

		if cmd.metadir != "" {
			return fmt.Errorf("offline parameter metadir found, not compatible with -portable")
		}

		if cmd.datadir != "" {
			return fmt.Errorf("offline parameter datadir found, not compatible with -portable")
		}

		if cmd.restoreRetention == "" {
			cmd.restoreRetention = cmd.backupRetention
		}

		if cmd.portable {
			var err error
			cmd.manifestMeta, cmd.manifestFiles, err = backup_util.LoadIncremental(cmd.backupFilesPath)
			if err != nil {
				return fmt.Errorf("restore failed while processing manifest files: %s", err.Error())
			}
		}
	} else {
		// validate the arguments
		if cmd.metadir == "" && cmd.destinationDatabase == "" {
			return fmt.Errorf("-metadir or -destinationDatabase are required to restore")
		}

		if cmd.destinationDatabase != "" && cmd.datadir == "" {
			return fmt.Errorf("-datadir is required to restore")
		}

		if cmd.shard != 0 {
			if cmd.destinationDatabase == "" {
				return fmt.Errorf("-destinationDatabase is required to restore shard")
			}
			if cmd.backupRetention == "" {
				return fmt.Errorf("-retention is required to restore shard")
			}
		} else if cmd.backupRetention != "" && cmd.destinationDatabase == "" {
			return fmt.Errorf("-destinationDatabase is required to restore retention policy")
		}
	}

	return nil
}

// unpackMeta reads the metadata from the backup directory and initializes a raft
// cluster and replaces the root metadata.
func (cmd *Command) unpackMeta() error {
	// find the meta file
	metaFiles, err := filepath.Glob(filepath.Join(cmd.backupFilesPath, backup_util.Metafile+".*"))
	if err != nil {
		return err
	}

	if len(metaFiles) == 0 {
		return fmt.Errorf("no metastore backups in %s", cmd.backupFilesPath)
	}

	latest := metaFiles[len(metaFiles)-1]

	fmt.Fprintf(cmd.Stdout, "Using metastore snapshot: %v\n", latest)
	// Read the metastore backup
	f, err := os.Open(latest)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, f); err != nil {
		return fmt.Errorf("copy: %s", err)
	}

	b := buf.Bytes()
	var i int

	// Make sure the file is actually a meta store backup file
	magic := binary.BigEndian.Uint64(b[:8])
	if magic != snapshotter.BackupMagicHeader {
		return fmt.Errorf("invalid metadata file")
	}
	i += 8

	// Size of the meta store bytes
	length := int(binary.BigEndian.Uint64(b[i : i+8]))
	i += 8
	metaBytes := b[i : i+length]
	i += int(length)

	// Size of the node.json bytes
	length = int(binary.BigEndian.Uint64(b[i : i+8]))
	i += 8
	nodeBytes := b[i : i+length]

	// Unpack into metadata.
	var data meta.Data
	if err := data.UnmarshalBinary(metaBytes); err != nil {
		return fmt.Errorf("unmarshal: %s", err)
	}

	// Copy meta config and remove peers so it starts in single mode.
	c := cmd.MetaConfig
	c.Dir = cmd.metadir

	// Create the meta dir
	if err := os.MkdirAll(c.Dir, 0700); err != nil {
		return err
	}

	// Write node.json back to meta dir
	if err := ioutil.WriteFile(filepath.Join(c.Dir, "node.json"), nodeBytes, 0655); err != nil {
		return err
	}

	client := meta.NewClient(c)
	if err := client.Open(); err != nil {
		return err
	}
	defer client.Close()

	// Force set the full metadata.
	if err := client.SetData(&data); err != nil {
		return fmt.Errorf("set data: %s", err)
	}

	// remove the raft.db file if it exists
	err = os.Remove(filepath.Join(cmd.metadir, "raft.db"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// remove the node.json file if it exists
	err = os.Remove(filepath.Join(cmd.metadir, "node.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return nil
}

func (cmd *Command) updateMetaPortable() error {
	var metaBytes []byte
	fileName := filepath.Join(cmd.backupFilesPath, cmd.manifestMeta.FileName)

	fileBytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}

	var ep backup_util.PortablePacker
	ep.UnmarshalBinary(fileBytes)

	metaBytes = ep.Data

	req := &snapshotter.Request{
		Type:                   snapshotter.RequestMetaStoreUpdate,
		BackupDatabase:         cmd.sourceDatabase,
		RestoreDatabase:        cmd.destinationDatabase,
		BackupRetentionPolicy:  cmd.backupRetention,
		RestoreRetentionPolicy: cmd.restoreRetention,
		UploadSize:             int64(len(metaBytes)),
	}

	shardIDMap, err := cmd.client.UpdateMeta(req, bytes.NewReader(metaBytes))
	cmd.shardIDMap = shardIDMap
	return err

}

// updateMetaLive takes a metadata backup and sends it to the influx server
// for a live merger of metadata.
func (cmd *Command) updateMetaLegacy() error {

	var metaBytes []byte

	// find the meta file
	metaFiles, err := filepath.Glob(filepath.Join(cmd.backupFilesPath, backup_util.Metafile+".*"))
	if err != nil {
		return err
	}

	if len(metaFiles) == 0 {
		return fmt.Errorf("no metastore backups in %s", cmd.backupFilesPath)
	}

	fileName := metaFiles[len(metaFiles)-1]
	cmd.StdoutLogger.Printf("Using metastore snapshot: %v\n", fileName)
	metaBytes, err = backup_util.GetMetaBytes(fileName)
	if err != nil {
		return err
	}

	req := &snapshotter.Request{
		Type:                   snapshotter.RequestMetaStoreUpdate,
		BackupDatabase:         cmd.sourceDatabase,
		RestoreDatabase:        cmd.destinationDatabase,
		BackupRetentionPolicy:  cmd.backupRetention,
		RestoreRetentionPolicy: cmd.restoreRetention,
		UploadSize:             int64(len(metaBytes)),
	}

	shardIDMap, err := cmd.client.UpdateMeta(req, bytes.NewReader(metaBytes))
	cmd.shardIDMap = shardIDMap
	return err
}

// unpackShard will look for all backup files in the path matching this shard ID
// and restore them to the data dir
func (cmd *Command) unpackShard(shard uint64) error {
	shardID := strconv.FormatUint(shard, 10)
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(cmd.datadir, cmd.destinationDatabase, cmd.restoreRetention, shardID)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("shard already present: %s", restorePath)
	}

	id, err := strconv.ParseUint(shardID, 10, 64)
	if err != nil {
		return err
	}

	// find the shard backup files
	pat := filepath.Join(cmd.backupFilesPath, fmt.Sprintf(backup_util.BackupFilePattern, cmd.destinationDatabase, cmd.restoreRetention, id))
	return cmd.unpackFiles(pat + ".*")
}

func (cmd *Command) uploadShardsPortable() error {
	for _, file := range cmd.manifestFiles {
		if cmd.sourceDatabase == "" || cmd.sourceDatabase == file.Database {
			if cmd.backupRetention == "" || cmd.backupRetention == file.Policy {
				if cmd.shard == 0 || cmd.shard == file.ShardID {
					cmd.StdoutLogger.Printf("Restoring shard %d live from backup %s\n", file.ShardID, file.FileName)
					f, err := os.Open(filepath.Join(cmd.backupFilesPath, file.FileName))
					if err != nil {
						f.Close()
						return err
					}
					gr, err := gzip.NewReader(f)
					if err != nil {
						f.Close()
						return err
					}
					tr := tar.NewReader(gr)
					targetDB := cmd.destinationDatabase
					if targetDB == "" {
						targetDB = file.Database
					}

					if err := cmd.client.UploadShard(file.ShardID, cmd.shardIDMap[file.ShardID], targetDB, cmd.restoreRetention, tr); err != nil {
						f.Close()
						return err
					}
					f.Close()
				}
			}
		}
	}
	return nil
}

// unpackFiles will look for backup files matching the pattern and restore them to the data dir
func (cmd *Command) uploadShardsLegacy() error {
	// find the destinationDatabase backup files
	pat := fmt.Sprintf("%s.*", filepath.Join(cmd.backupFilesPath, cmd.sourceDatabase))
	cmd.StdoutLogger.Printf("Restoring live from backup %s\n", pat)
	backupFiles, err := filepath.Glob(pat)
	if err != nil {
		return err
	}
	if len(backupFiles) == 0 {
		return fmt.Errorf("no backup files in %s", cmd.backupFilesPath)
	}

	for _, fn := range backupFiles {
		parts := strings.Split(fn, ".")

		if len(parts) != 4 {
			cmd.StderrLogger.Printf("Skipping mis-named backup file: %s", fn)
		}
		shardID, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			return err
		}
		f, err := os.Open(fn)
		if err != nil {
			return err
		}
		tr := tar.NewReader(f)
		if err := cmd.client.UploadShard(shardID, cmd.shardIDMap[shardID], cmd.destinationDatabase, cmd.restoreRetention, tr); err != nil {
			f.Close()
			return err
		}
		f.Close()
	}

	return nil
}

// unpackDatabase will look for all backup files in the path matching this destinationDatabase
// and restore them to the data dir
func (cmd *Command) unpackDatabase() error {
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(cmd.datadir, cmd.destinationDatabase)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("database already present: %s", restorePath)
	}

	// find the database backup files
	pat := filepath.Join(cmd.backupFilesPath, cmd.destinationDatabase)
	return cmd.unpackFiles(pat + ".*")
}

// unpackRetention will look for all backup files in the path matching this retention
// and restore them to the data dir
func (cmd *Command) unpackRetention() error {
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(cmd.datadir, cmd.destinationDatabase, cmd.restoreRetention)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("retention already present: %s", restorePath)
	}

	// find the retention backup files
	pat := filepath.Join(cmd.backupFilesPath, cmd.destinationDatabase)
	return cmd.unpackFiles(fmt.Sprintf("%s.%s.*", pat, cmd.restoreRetention))
}

// unpackFiles will look for backup files matching the pattern and restore them to the data dir
func (cmd *Command) unpackFiles(pat string) error {
	cmd.StdoutLogger.Printf("Restoring offline from backup %s\n", pat)

	backupFiles, err := filepath.Glob(pat)
	if err != nil {
		return err
	}

	if len(backupFiles) == 0 {
		return fmt.Errorf("no backup files for %s in %s", pat, cmd.backupFilesPath)
	}

	for _, fn := range backupFiles {
		if err := cmd.unpackTar(fn); err != nil {
			return err
		}
	}

	return nil
}

// unpackTar will restore a single tar archive to the data dir
func (cmd *Command) unpackTar(tarFile string) error {
	f, err := os.Open(tarFile)
	if err != nil {
		return err
	}
	defer f.Close()

	// should get us ["db","rp", "00001", "00"]
	pathParts := strings.Split(filepath.Base(tarFile), ".")
	if len(pathParts) != 4 {
		return fmt.Errorf("backup tarfile name incorrect format")
	}

	shardPath := filepath.Join(cmd.datadir, pathParts[0], pathParts[1], strings.Trim(pathParts[2], "0"))
	os.MkdirAll(shardPath, 0755)

	return tarstream.Restore(f, shardPath)
}

// printUsage prints the usage message to STDERR.
func (cmd *Command) printUsage() {
	fmt.Fprintf(cmd.Stdout, `
Uses backups from the PATH to restore the metastore, databases, retention policies, or specific shards.
Default mode requires the instance to be stopped before running, and will wipe all databases from the system
(e.g., for disaster recovery).  The improved online and portable modes require the instance to be running,
and the database name used must not already exist.

Usage: influxd restore [-portable] [flags] PATH

The default mode consumes files in an OSS only file format. PATH is a directory containing the backup data

Options:
    -metadir <path>
            Optional. If set the metastore will be recovered to the given path.
    -datadir <path>
            Optional. If set the restore process will recover the specified
            database, retention policy or shard to the given directory.
    -database <name>
            Optional. Required if no metadir given. Will restore a single database's data.
    -retention <name>
            Optional. If given, -database is required. Will restore the retention policy's
            data.
    -shard <id>
            Optional. If given, -database and -retention are required. Will restore the shard's
            data.
    -online
            Optional. If given, the restore will be done using the new process, detailed below.  All other arguments
            above should be omitted.

The -portable restore mode consumes files in an improved format that includes a file manifest.

Options:
    -host  <host:port>
            The host to connect to and perform a snapshot of. Defaults to '127.0.0.1:8088'.
    -db    <name>
            Identifies the database from the backup that will be restored.
    -newdb <name>
            The name of the database into which the archived data will be imported on the target system.
            If not given, then the value of -db is used.  The new database name must be unique to the target system.
    -rp    <name>
            Identifies the retention policy from the backup that will be restored.  Requires that -db is set.
    -newrp <name>
            The name of the retention policy that will be created on the target system. Requires that -rp is set.
            If not given, the value of -rp is used.
    -shard <id>
            Optional.  If given, -db and -rp are required.  Will restore the single shard's data.
`)
}
