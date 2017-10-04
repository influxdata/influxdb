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

	"encoding/json"
	"github.com/influxdata/influxdb/cmd/influxd/backup"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/snapshotter"
	"github.com/influxdata/influxdb/tcp"
)

// Command represents the program execution for "influxd restore".
type Command struct {
	// The logger passed to the ticker during execution.
	StdoutLogger *log.Logger
	StderrLogger *log.Logger

	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer

	host string
	path string

	backupFilesPath     string
	metadir             string
	datadir             string
	destinationDatabase string
	sourceDatabase      string
	retention           string
	shard               string
	liveUpdate          bool

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
	if err := cmd.parseFlags(args); err != nil {
		return err
	}

	if cmd.metadir != "" {
		if err := cmd.unpackMeta(); err != nil {
			return err
		}
	}

	if cmd.shard != "" {
		return cmd.unpackShard(cmd.shard)
	} else if cmd.retention != "" {
		return cmd.unpackRetention()
	} else if cmd.datadir != "" {
		return cmd.unpackDatabase()
	}
	return nil
}

// parseFlags parses and validates the command line arguments.
func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.StringVar(&cmd.metadir, "metadir", "", "")
	fs.StringVar(&cmd.datadir, "datadir", "", "")
	fs.StringVar(&cmd.destinationDatabase, "database", "", "")
	fs.StringVar(&cmd.sourceDatabase, "origindb", "", "")
	fs.StringVar(&cmd.destinationDatabase, "newdb", "", "")
	fs.StringVar(&cmd.retention, "retention", "", "")
	fs.StringVar(&cmd.shard, "shard", "", "")
	fs.BoolVar(&cmd.liveUpdate, "online", false, "")
	fs.SetOutput(cmd.Stdout)
	fs.Usage = cmd.printUsage
	if err := fs.Parse(args); err != nil {
		return err
	}

	cmd.MetaConfig = meta.NewConfig()
	cmd.MetaConfig.Dir = cmd.metadir

	// Require output path.
	cmd.backupFilesPath = fs.Arg(0)
	if cmd.backupFilesPath == "" {
		return fmt.Errorf("path with backup files required")
	}

	if cmd.liveUpdate {
		// validate the arguments
		if cmd.destinationDatabase == "" {
			return fmt.Errorf("-newdb is a required parameter when using --online")
		}

		if cmd.sourceDatabase == "" {
			cmd.sourceDatabase = cmd.destinationDatabase
		}
	} else {
		// validate the arguments
		if cmd.metadir == "" && cmd.destinationDatabase == "" {
			return fmt.Errorf("-metadir or -destinationDatabase are required to restore")
		}

		if cmd.destinationDatabase != "" && cmd.datadir == "" {
			return fmt.Errorf("-datadir is required to restore")
		}

		if cmd.shard != "" {
			if cmd.destinationDatabase == "" {
				return fmt.Errorf("-destinationDatabase is required to restore shard")
			}
			if cmd.retention == "" {
				return fmt.Errorf("-retention is required to restore shard")
			}
		} else if cmd.retention != "" && cmd.destinationDatabase == "" {
			return fmt.Errorf("-destinationDatabase is required to restore retention policy")
		}
	}

	return nil
}

// unpackMeta reads the metadata from the backup directory and initializes a raft
// cluster and replaces the root metadata.
func (cmd *Command) unpackMeta() error {
	// find the meta file
	metaFiles, err := filepath.Glob(filepath.Join(cmd.backupFilesPath, backup.Metafile+".*"))
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
	if os.MkdirAll(c.Dir, 0700); err != nil {
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

// updateMetaLive takes a metadata backup and sends it to the influx server
// for a live merger of metadata.
func (cmd *Command) updateMetaLive() error {
	// find the meta file
	metaFiles, err := filepath.Glob(filepath.Join(cmd.backupFilesPath, backup.Metafile+".*"))
	if err != nil {
		return err
	}

	if len(metaFiles) == 0 {
		return fmt.Errorf("no metastore backups in %s", cmd.backupFilesPath)
	}

	latest := metaFiles[len(metaFiles)-1]

	cmd.StdoutLogger.Printf("Using metastore snapshot: %v\n", latest)
	// Read the metastore backup
	req := &snapshotter.Request{
		Type:     snapshotter.RequestMetaStoreUpdate,
		Database: cmd.destinationDatabase,
	}

	metaBytes, err := cmd.getMetaBytes(latest)

	resp, err := cmd.updateMetaRemote(req, bytes.NewReader(metaBytes), int64(len(metaBytes)))
	if err != nil {
		return err
	}

	header := binary.BigEndian.Uint64(resp[:8])
	npairs := binary.BigEndian.Uint64(resp[8:16])

	if npairs == 0 {
		return fmt.Errorf("DB metadata not changed. database may already exist")
	}

	pairs := resp[16:]

	if header != snapshotter.BackupMagicHeader {
		return fmt.Errorf("Response did not contain the proper header tag.")
	}

	if len(pairs)%16 != 0 || (len(pairs)/8)%2 != 0 {
		return fmt.Errorf("expected an even number of integer pairs in update meta repsonse")
	}

	cmd.shardIDMap = make(map[uint64]uint64)
	for i := 0; i < int(npairs); i++ {
		offset := i * 16
		k := binary.BigEndian.Uint64(pairs[offset : offset+8])
		v := binary.BigEndian.Uint64(pairs[offset+8 : offset+16])
		cmd.shardIDMap[k] = v
	}

	return err
}

func (cmd *Command) getMetaBytes(fname string) ([]byte, error) {
	f, err := os.Open(fname)
	if err != nil {
		return []byte{}, err
	}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, f); err != nil {
		return []byte{}, fmt.Errorf("copy: %s", err)
	}

	b := buf.Bytes()
	var i int

	// Make sure the file is actually a meta store backup file
	magic := binary.BigEndian.Uint64(b[:8])
	if magic != snapshotter.BackupMagicHeader {
		return []byte{}, fmt.Errorf("invalid metadata file")
	}
	i += 8

	// Size of the meta store bytes
	length := int(binary.BigEndian.Uint64(b[i : i+8]))
	i += 8
	metaBytes := b[i : i+length]

	return metaBytes, nil
}

// upload takes a request object, attaches a Base64 encoding to the request, and sends it to the snapshotter service.
func (cmd *Command) updateMetaRemote(req *snapshotter.Request, upStream io.Reader, nbytes int64) ([]byte, error) {

	req.UploadSize = nbytes
	var err error

	var b bytes.Buffer

	// Connect to snapshotter service.
	conn, err := tcp.Dial("tcp", cmd.host, snapshotter.MuxHeader)
	if err != nil {
		return b.Bytes(), err
	}
	defer conn.Close()

	conn.Write([]byte{byte(req.Type)})

	if req.Type != snapshotter.RequestShardUpdate { // Write the request
		if err := json.NewEncoder(conn).Encode(req); err != nil {
			return b.Bytes(), fmt.Errorf("encode snapshot request: %s", err)
		}
	}

	if n, err := io.Copy(conn, upStream); (err != nil && err != io.EOF) || n != req.UploadSize {
		return b.Bytes(), fmt.Errorf("error uploading file: err=%v, n=%d, uploadSize: %d", err, n, req.UploadSize)
	}

	//var response bytes.Buffer
	//// Read snapshot from the connection
	cmd.StdoutLogger.Printf("wrote %d bytes", req.UploadSize)

	b.Reset()
	if n, err := b.ReadFrom(conn); err != nil || n == 0 {
		return b.Bytes(), fmt.Errorf("copy backup to file: err=%v, n=%d", err, n)
	}

	return b.Bytes(), nil
}

// unpackShard will look for all backup files in the path matching this shard ID
// and restore them to the data dir
func (cmd *Command) unpackShard(shardID string) error {
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(cmd.datadir, cmd.destinationDatabase, cmd.retention, shardID)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("shard already present: %s", restorePath)
	}

	id, err := strconv.ParseUint(shardID, 10, 64)
	if err != nil {
		return err
	}

	// find the shard backup files
	pat := filepath.Join(cmd.backupFilesPath, fmt.Sprintf(backup.BackupFilePattern, cmd.destinationDatabase, cmd.retention, id))
	return cmd.unpackFiles(pat + ".*")
}

// unpackFiles will look for backup files matching the pattern and restore them to the data dir
func (cmd *Command) uploadShardsLive() error {

	// gets DB, RP, shardID from a path string.
	//a := strings.Split(path, string(filepath.Separator))
	//if len(a) != 3 {
	//	return "", "", fmt.Errorf("expected destinationDatabase, retention policy, and shard id in path: %s", path)
	//}

	// find the destinationDatabase backup files
	pat := fmt.Sprintf("%s.*", filepath.Join(cmd.backupFilesPath, cmd.sourceDatabase))

	cmd.StdoutLogger.Printf("Restoring live from backup %s\n", pat)

	backupFiles, err := filepath.Glob(pat)
	if err != nil {
		return err
	}

	if len(backupFiles) == 0 {
		return fmt.Errorf("no backup files for %s in %s", pat, cmd.backupFilesPath)
	}

	for _, fn := range backupFiles {
		cmd.StdoutLogger.Printf("unpacking %s\n", fn)
		parts := strings.Split(fn, ".")

		if len(parts) != 4 {
			cmd.StderrLogger.Printf("Skipping mis-named backup file: %s", fn)
		}
		shardID, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			return err
		}

		newShardID := cmd.shardIDMap[shardID]

		conn, err := tcp.Dial("tcp", cmd.host, snapshotter.MuxHeader)
		if err != nil {
			return err
		}

		conn.Write([]byte{byte(snapshotter.RequestShardUpdate)})

		// 0.  write the shard ID to pw
		shardBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(shardBytes, newShardID)
		conn.Write(shardBytes)
		// 1.  open TAR reader for file
		f, err := os.Open(fn)

		if err != nil {
			return err
		}
		tr := tar.NewReader(f)

		tw := tar.NewWriter(conn)

		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			} else if err != nil {
				tw.Close()
				f.Close()
				conn.Close()
				return err
			}

			names := strings.Split(hdr.Name, "/")
			hdr.Name = filepath.ToSlash(filepath.Join(cmd.destinationDatabase, names[1], strconv.FormatUint(newShardID, 10), names[3]))

			tw.WriteHeader(hdr)
			if _, err := io.Copy(tw, tr); err != nil {
				tw.Close()
				f.Close()
				conn.Close()
				return err
			}
		}
		tw.Close()
		f.Close()
		conn.Close()
	}

	return nil
}

// unpackDatabase will look for all backup files in the path matching this destinationDatabase
// and restore them to the data dir
func (cmd *Command) unpackDatabase() error {
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(cmd.datadir, cmd.destinationDatabase)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("destinationDatabase already present: %s", restorePath)
	}

	// find the destinationDatabase backup files
	pat := filepath.Join(cmd.backupFilesPath, cmd.destinationDatabase)
	return cmd.unpackFiles(pat + ".*")
}

// unpackRetention will look for all backup files in the path matching this retention
// and restore them to the data dir
func (cmd *Command) unpackRetention() error {
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(cmd.datadir, cmd.destinationDatabase, cmd.retention)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("retention already present: %s", restorePath)
	}

	// find the retention backup files
	pat := filepath.Join(cmd.backupFilesPath, cmd.destinationDatabase)
	return cmd.unpackFiles(fmt.Sprintf("%s.%s.*", pat, cmd.retention))
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

	tr := tar.NewReader(f)

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		if err := cmd.unpackFile(tr, hdr.Name); err != nil {
			return err
		}
	}
}

// unpackFile will copy the current file from the tar archive to the data dir
func (cmd *Command) unpackFile(tr *tar.Reader, fileName string) error {
	nativeFileName := filepath.FromSlash(fileName)
	fn := filepath.Join(cmd.datadir, nativeFileName)
	cmd.StdoutLogger.Printf("unpacking %s\n", fn)

	if err := os.MkdirAll(filepath.Dir(fn), 0777); err != nil {
		return fmt.Errorf("error making restore dir: %s", err.Error())
	}

	ff, err := os.Create(fn)
	if err != nil {
		return err
	}
	defer ff.Close()

	if _, err := io.Copy(ff, tr); err != nil {
		return err
	}

	return nil
}

// printUsage prints the usage message to STDERR.
func (cmd *Command) printUsage() {
	fmt.Fprintf(cmd.Stdout, `Uses backups from the PATH to restore the metastore, databases,
retention policies, or specific shards.  Offline mode requires the instance to be stopped before running, and will wipe
	all databases from the system (e.g., for disaster recovery).  Online mode requires the instance to be running,
	and the database name used must not already exist.

Usage: influxd restore [flags] PATH

Offline (legacy) mode:
    -metadir <path>
            Optional. If set the metastore will be recovered to the given path.
    -datadir <path>
            Optional. If set the restore process will recover the specified
            destinationDatabase, retention policy or shard to the given directory.
    -database <name>
            Optional. Required if no metadir given. Will restore the destinationDatabase
            TSM files.
    -retention <name>
            Optional. If given, destinationDatabase is required. Will restore the retention policy's
            TSM files.
    -shard <id>
            Optional. If given, destinationDatabase and retention are required. Will restore the shard's
            TSM files.
Online import mode:
	-online
		    Optional. If set, the update will be done on a live influxdb instance.  All other databases in the system
	        will be unaltered.
	-origindb
	        Required if -online set.  The name of the archived database that you want to import.
	-newdb
	        Optional, used only if -online set.  The name of the database in which the archived data will be imported.
	        If not given, then the value of -origindb is used.  The new database name must be unique to the target system.

`)
}
