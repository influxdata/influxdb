package restore

import (
	"archive/tar"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"

	"github.com/influxdb/influxdb/cmd/influxd/backup"
	"github.com/influxdb/influxdb/meta"
)

// Command represents the program execution for "influxd restore".
type Command struct {
	Stdout io.Writer
	Stderr io.Writer

	backupFilesPath string
	metadir         string
	datadir         string
	database        string
	retention       string
	shard           string

	// TODO: when the new meta stuff is done this should not be exported or be gone
	MetaConfig *meta.Config
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
	}

	return cmd.unpackDatabase()
}

// parseFlags parses and validates the command line arguments.
func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.StringVar(&cmd.metadir, "metadir", "", "")
	fs.StringVar(&cmd.datadir, "datadir", "", "")
	fs.StringVar(&cmd.database, "database", "", "")
	fs.StringVar(&cmd.retention, "retention", "", "")
	fs.StringVar(&cmd.shard, "shard", "", "")
	fs.SetOutput(cmd.Stderr)
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

	// validate the arguments
	if cmd.metadir == "" && cmd.database == "" {
		return fmt.Errorf("either a metadir or database are required to restore")
	}

	if cmd.database != "" || cmd.retention != "" || cmd.shard != "" {
		if cmd.datadir == "" {
			return fmt.Errorf("datadir is required to restore")
		}
	}

	if cmd.shard != "" {
		if cmd.database == "" {
			return fmt.Errorf("database is required to restore shard")
		}
		if cmd.retention == "" {
			return fmt.Errorf("retention is required to restore shard")
		}
	} else if cmd.retention != "" {
		if cmd.database == "" {
			return fmt.Errorf("database is required to restore retention policy")
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

	// Read the metastore backup
	f, err := os.Open(metaFiles[len(metaFiles)-1])
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, f); err != nil {
		return fmt.Errorf("copy: %s", err)
	}

	// Unpack into metadata.
	var data meta.Data
	if err := data.UnmarshalBinary(buf.Bytes()); err != nil {
		return fmt.Errorf("unmarshal: %s", err)
	}

	// Initialize meta store.
	store := meta.NewStore(cmd.MetaConfig)
	store.RaftListener = newNopListener()
	store.ExecListener = newNopListener()
	store.RPCListener = newNopListener()

	// Determine advertised address.
	_, port, err := net.SplitHostPort(cmd.MetaConfig.BindAddress)
	if err != nil {
		return fmt.Errorf("split bind address: %s", err)
	}
	hostport := net.JoinHostPort(cmd.MetaConfig.Hostname, port)

	// Resolve address.
	addr, err := net.ResolveTCPAddr("tcp", hostport)
	if err != nil {
		return fmt.Errorf("resolve tcp: addr=%s, err=%s", hostport, err)
	}
	store.Addr = addr
	store.RemoteAddr = addr

	// Open the meta store.
	if err := store.Open(); err != nil {
		return fmt.Errorf("open store: %s", err)
	}
	defer store.Close()

	// Wait for the store to be ready or error.
	select {
	case <-store.Ready():
	case err := <-store.Err():
		return err
	}

	// Force set the full metadata.
	if err := store.SetData(&data); err != nil {
		return fmt.Errorf("set data: %s", err)
	}

	return nil
}

// unpackShard will look for all backup files in the path matching this shard ID
// and restore them to the data dir
func (cmd *Command) unpackShard(shardID string) error {
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(cmd.datadir, cmd.database, cmd.retention, shardID)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("shard already present: %s", restorePath)
	}

	// find the shard backup files
	pat := filepath.Join(cmd.backupFilesPath, fmt.Sprintf(backup.BackupFilePattern, cmd.database, cmd.retention, shardID))
	return cmd.unpackFiles(pat + ".*")
}

// unpackDatabase will look for all backup files in the path matching this database
// and restore them to the data dir
func (cmd *Command) unpackDatabase() error {
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(cmd.datadir, cmd.database)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("database already present: %s", restorePath)
	}

	// find the database backup files
	pat := filepath.Join(cmd.backupFilesPath, cmd.database)
	return cmd.unpackFiles(pat + ".*")
}

// unpackRetention will look for all backup files in the path matching this retention
// and restore them to the data dir
func (cmd *Command) unpackRetention() error {
	// make sure the shard isn't already there so we don't clobber anything
	restorePath := filepath.Join(cmd.datadir, cmd.database, cmd.retention)
	if _, err := os.Stat(restorePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("retention already present: %s", restorePath)
	}

	// find the retention backup files
	pat := filepath.Join(cmd.backupFilesPath, cmd.database)
	return cmd.unpackFiles(fmt.Sprintf("%s.%s.*", pat, cmd.retention))
}

// unpackFiles will look for backup files matching the pattern and restore them to the data dir
func (cmd *Command) unpackFiles(pat string) error {
	fmt.Printf("restoring from backup %s\n", pat)

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
	fn := filepath.Join(cmd.datadir, fileName)
	fmt.Printf("unpacking %s", fn)

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
	fmt.Fprintf(cmd.Stderr, `usage: influxd restore [flags] PATH

restore uses backups from the PATH to restore the metastore, databases, retention policies, or specific shards. The InfluxDB process must not be running during restore.

        -metadir <path>
                    	Optional. If set the metastore will be recovered to the given path.
        -datadir <path>
        				Optional. If set the restore process will recover the specified
        				database, retention policy or shard to the given directory.
       	-database <name>
       					Optional. Required if no metadir given. Will restore the database TSM files.
       	-retention <name>
       					Optional. If given, database is required. Will restore the retention policy's TSM files.
       	-shard <id>
       					Optional. If given, database and retention are required. Will restore the shard's TSM files.
`)
}

type nopListener struct {
	closing chan struct{}
}

func newNopListener() *nopListener {
	return &nopListener{make(chan struct{})}
}

func (ln *nopListener) Accept() (net.Conn, error) {
	<-ln.closing
	return nil, errors.New("listener closing")
}

func (ln *nopListener) Close() error {
	if ln.closing != nil {
		close(ln.closing)
		ln.closing = nil
	}
	return nil
}

func (ln *nopListener) Addr() net.Addr { return nil }
