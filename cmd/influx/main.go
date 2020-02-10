package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/cmd/influx/internal"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/internal/fs"
	"github.com/influxdata/influxdb/kit/cli"
	"github.com/influxdata/influxdb/kv"
	"github.com/influxdata/influxdb/pkg/httpc"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const maxTCPConnections = 128

func main() {
	influxCmd := influxCmd()
	if err := influxCmd.Execute(); err != nil {
		seeHelp(influxCmd, nil)
		os.Exit(1)
	}
}

var (
	httpClient *httpc.Client
)

func newHTTPClient() (*httpc.Client, error) {
	if httpClient != nil {
		return httpClient, nil
	}

	c, err := http.NewHTTPClient(flags.host, flags.token, flags.skipVerify)
	if err != nil {
		return nil, err
	}

	httpClient = c
	return httpClient, nil
}

type (
	runEWrapFn func(fn func(*cobra.Command, []string) error) func(*cobra.Command, []string) error

	genericCLIOptFn func(*genericCLIOpts)
)

type genericCLIOpts struct {
	in         io.Reader
	w          io.Writer
	runEWrapFn runEWrapFn
}

func (o genericCLIOpts) newCmd(use string, runE func(*cobra.Command, []string) error) *cobra.Command {
	cmd := &cobra.Command{
		Args: cobra.NoArgs,
		Use:  use,
		RunE: runE,
	}
	if runE != nil && o.runEWrapFn != nil {
		cmd.RunE = o.runEWrapFn(runE)
	}
	cmd.SetOut(o.w)
	return cmd
}

func (o genericCLIOpts) newTabWriter() *internal.TabWriter {
	return internal.NewTabWriter(o.w)
}

func in(r io.Reader) genericCLIOptFn {
	return func(o *genericCLIOpts) {
		o.in = r
	}
}

func out(w io.Writer) genericCLIOptFn {
	return func(o *genericCLIOpts) {
		o.w = w
	}
}

func runEWrap(fn runEWrapFn) genericCLIOptFn {
	return func(opts *genericCLIOpts) {
		opts.runEWrapFn = fn
	}
}

var flags struct {
	token      string
	host       string
	local      bool
	skipVerify bool
}

func influxCmd(opts ...genericCLIOptFn) *cobra.Command {
	opt := genericCLIOpts{
		in: os.Stdin,
		w:  os.Stdout,
	}
	for _, o := range opts {
		o(&opt)
	}

	cmd := opt.newCmd("influx", nil)
	cmd.Short = "Influx Client"
	cmd.SilenceUsage = true

	setViperOptions()

	runEWrapper := runEWrap(wrapCheckSetup)

	cmd.AddCommand(
		cmdAuth(),
		cmdBackup(),
		cmdBucket(runEWrapper),
		cmdDelete(),
		cmdOrganization(runEWrapper),
		cmdPing(),
		cmdPkg(runEWrapper),
		cmdQuery(),
		cmdTranspile(),
		cmdREPL(),
		cmdSecret(runEWrapper),
		cmdSetup(),
		cmdTask(),
		cmdUser(runEWrapper),
		cmdWrite(),
	)

	fOpts := flagOpts{
		{
			DestP:      &flags.token,
			Flag:       "token",
			Short:      't',
			Desc:       "API token to be used throughout client calls",
			Persistent: true,
		},
		{
			DestP:      &flags.host,
			Flag:       "host",
			Default:    "http://localhost:9999",
			Desc:       "HTTP address of Influx",
			Persistent: true,
		},
	}
	fOpts.mustRegister(cmd)

	// this is after the flagOpts register b/c we don't want to show the default value
	// in the usage display. This will add it as the token value, then if a token flag
	// is provided too, the flag will take precedence.
	flags.token = getTokenFromDefaultPath()

	cmd.PersistentFlags().BoolVar(&flags.local, "local", false, "Run commands locally against the filesystem")
	cmd.PersistentFlags().BoolVar(&flags.skipVerify, "skip-verify", false, "SkipVerify controls whether a client verifies the server's certificate chain and host name.")

	// Update help description for all commands in command tree
	walk(cmd, func(c *cobra.Command) {
		c.Flags().BoolP("help", "h", false, fmt.Sprintf("Help for the %s command ", c.Name()))
	})

	return cmd
}

func fetchSubCommand(parent *cobra.Command, args []string) *cobra.Command {
	var err error
	var cmd *cobra.Command

	// Workaround FAIL with "go test -v" or "cobra.test -test.v", see #155
	if args == nil && filepath.Base(os.Args[0]) != "cobra.test" {
		args = os.Args[1:]
	}

	if parent.TraverseChildren {
		cmd, _, err = parent.Traverse(args)
	} else {
		cmd, _, err = parent.Find(args)
	}
	// return nil if any errs
	if err != nil {
		return nil
	}
	return cmd
}

func seeHelp(c *cobra.Command, args []string) {
	if c = fetchSubCommand(c, args); c == nil {
		return //return here, since cobra already handles the error
	}
	c.Printf("See '%s -h' for help\n", c.CommandPath())
}

func defaultTokenPath() (string, string, error) {
	dir, err := fs.InfluxDir()
	if err != nil {
		return "", "", err
	}
	return filepath.Join(dir, http.DefaultTokenFile), dir, nil
}

func getTokenFromDefaultPath() string {
	path, _, err := defaultTokenPath()
	if err != nil {
		return ""
	}
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func writeTokenToPath(tok, path, dir string) error {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}
	return ioutil.WriteFile(path, []byte(tok), 0600)
}

func checkSetup(host string) error {
	s := &http.SetupService{
		Addr:               flags.host,
		InsecureSkipVerify: flags.skipVerify,
	}

	isOnboarding, err := s.IsOnboarding(context.Background())
	if err != nil {
		return err
	}

	if isOnboarding {
		return fmt.Errorf("the instance at %q has not been setup. Please run `influx setup` before issuing any additional commands", host)
	}

	return nil
}

func wrapCheckSetup(fn func(*cobra.Command, []string) error) func(*cobra.Command, []string) error {
	return wrapErrorFmt(func(cmd *cobra.Command, args []string) error {
		err := fn(cmd, args)
		if err == nil {
			return nil
		}

		if setupErr := checkSetup(flags.host); setupErr != nil && influxdb.EUnauthorized != influxdb.ErrorCode(setupErr) {
			return setupErr
		}

		return err
	})
}

func wrapErrorFmt(fn func(*cobra.Command, []string) error) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		err := fn(cmd, args)
		if err == nil {
			return nil
		}

		return internal.ErrorFmt(err)
	}
}

// walk calls f for c and all of its children.
func walk(c *cobra.Command, f func(*cobra.Command)) {
	f(c)
	for _, c := range c.Commands() {
		walk(c, f)
	}
}

func newLocalKVService() (*kv.Service, error) {
	boltFile, err := fs.BoltFile()
	if err != nil {
		return nil, err
	}

	store := bolt.NewKVStore(zap.NewNop(), boltFile)
	if err := store.Open(context.Background()); err != nil {
		return nil, err
	}

	return kv.NewService(zap.NewNop(), store), nil
}

type organization struct {
	id, name string
}

func (o *organization) register(cmd *cobra.Command, persistent bool) {
	opts := flagOpts{
		{
			DestP:      &o.id,
			Flag:       "org-id",
			Desc:       "The ID of the organization",
			Persistent: persistent,
		},
		{
			DestP:      &o.name,
			Flag:       "org",
			Short:      'o',
			Desc:       "The name of the organization",
			Persistent: persistent,
		},
	}
	opts.mustRegister(cmd)
}

func (o *organization) getID(orgSVC influxdb.OrganizationService) (influxdb.ID, error) {
	if o.id != "" {
		influxOrgID, err := influxdb.IDFromString(o.id)
		if err != nil {
			return 0, fmt.Errorf("invalid org ID provided: %s", err.Error())
		}
		return *influxOrgID, nil
	} else if o.name != "" {
		org, err := orgSVC.FindOrganization(context.Background(), influxdb.OrganizationFilter{
			Name: &o.name,
		})
		if err != nil {
			return 0, fmt.Errorf("%v", err)
		}
		return org.ID, nil
	}
	return 0, fmt.Errorf("failed to locate an organization id")
}

func (o *organization) validOrgFlags() error {
	if o.id == "" && o.name == "" {
		return fmt.Errorf("must specify org-id, or org name")
	} else if o.id != "" && o.name != "" {
		return fmt.Errorf("must specify org-id, or org name not both")
	}
	return nil
}

type flagOpts []cli.Opt

func (f flagOpts) mustRegister(cmd *cobra.Command) {
	for i := range f {
		envVar := f[i].Flag
		if e := f[i].EnvVar; e != "" {
			envVar = e
		}

		f[i].Desc = fmt.Sprintf(
			"%s; Maps to env var $INFLUX_%s",
			f[i].Desc,
			strings.ToUpper(strings.Replace(envVar, "-", "_", -1)),
		)
	}
	cli.BindOptions(cmd, f)
}

func setViperOptions() {
	viper.SetEnvPrefix("INFLUX")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
}

func newBucketService() (influxdb.BucketService, error) {
	if flags.local {
		return newLocalKVService()
	}

	client, err := newHTTPClient()
	if err != nil {
		return nil, err
	}

	return &http.BucketService{
		Client: client,
	}, nil
}
