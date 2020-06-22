package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influx/config"
	"github.com/influxdata/influxdb/v2/cmd/influx/internal"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const maxTCPConnections = 10

var (
	version = "dev"
	commit  = "none"
	date    = time.Now().UTC().Format(time.RFC3339)
)

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

	userAgent := fmt.Sprintf(
		"influx/%s (%s) Sha/%s Date/%s",
		version, runtime.GOOS, commit, date,
	)

	opts := []httpc.ClientOptFn{
		httpc.WithUserAgentHeader(userAgent),
	}
	// This is useful for forcing tracing on a given endpoint.
	if flags.traceDebugID != "" {
		opts = append(opts, httpc.WithHeader("jaeger-debug-id", flags.traceDebugID))
	}

	c, err := http.NewHTTPClient(flags.Host, flags.Token, flags.skipVerify, opts...)
	if err != nil {
		return nil, err
	}

	httpClient = c
	return httpClient, nil
}

type (
	cobraRunEFn func(cmd *cobra.Command, args []string) error

	cobraRunEMiddleware func(fn cobraRunEFn) cobraRunEFn

	genericCLIOptFn func(*genericCLIOpts)
)

type genericCLIOpts struct {
	in   io.Reader
	w    io.Writer
	errW io.Writer

	runEWrapFn cobraRunEMiddleware
}

func (o genericCLIOpts) newCmd(use string, runE func(*cobra.Command, []string) error, useRunEMiddleware bool) *cobra.Command {
	cmd := &cobra.Command{
		Args: cobra.NoArgs,
		Use:  use,
		RunE: runE,
		FParseErrWhitelist: cobra.FParseErrWhitelist{
			// allows for unknown flags, parser does not crap the bed
			// when providing a flag that doesn't exist/match.
			UnknownFlags: true,
		},
	}

	canWrapRunE := runE != nil && o.runEWrapFn != nil
	if useRunEMiddleware && canWrapRunE {
		cmd.RunE = o.runEWrapFn(runE)
	} else if canWrapRunE {
		cmd.RunE = runE
	}

	cmd.SetOut(o.w)
	cmd.SetIn(o.in)
	cmd.SetErr(o.errW)
	return cmd
}

func (o genericCLIOpts) writeJSON(v interface{}) error {
	return writeJSON(o.w, v)
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

func err(w io.Writer) genericCLIOptFn {
	return func(o *genericCLIOpts) {
		o.errW = w
	}
}

func runEMiddlware(mw cobraRunEMiddleware) genericCLIOptFn {
	return func(o *genericCLIOpts) {
		o.runEWrapFn = mw
	}
}

type globalFlags struct {
	config.Config
	skipVerify   bool
	traceDebugID string
}

var flags globalFlags

type cmdInfluxBuilder struct {
	genericCLIOpts

	once sync.Once
}

func newInfluxCmdBuilder(optFns ...genericCLIOptFn) *cmdInfluxBuilder {
	builder := new(cmdInfluxBuilder)

	opt := genericCLIOpts{
		in:         os.Stdin,
		w:          os.Stdout,
		errW:       os.Stderr,
		runEWrapFn: checkSetupRunEMiddleware(&flags),
	}
	for _, optFn := range optFns {
		optFn(&opt)
	}

	builder.genericCLIOpts = opt
	return builder
}

func (b *cmdInfluxBuilder) cmd(childCmdFns ...func(f *globalFlags, opt genericCLIOpts) *cobra.Command) *cobra.Command {
	b.once.Do(func() {
		// enforce that viper options only ever get set once
		setViperOptions()
	})

	cmd := b.newCmd("influx", nil, false)
	cmd.Short = "Influx Client"
	cmd.SilenceUsage = true

	for _, childCmd := range childCmdFns {
		cmd.AddCommand(childCmd(&flags, b.genericCLIOpts))
	}

	fOpts := flagOpts{
		{
			DestP:      &flags.Token,
			Flag:       "token",
			Short:      't',
			Desc:       "API token to be used throughout client calls",
			Persistent: true,
		},
		{
			DestP:      &flags.Host,
			Flag:       "host",
			Desc:       "HTTP address of Influx",
			Persistent: true,
		},
		{
			DestP:      &flags.traceDebugID,
			Flag:       "trace-debug-id",
			Hidden:     true,
			Persistent: true,
		},
	}
	fOpts.mustRegister(cmd)

	// migration credential token
	migrateOldCredential()

	// this is after the flagOpts register b/c we don't want to show the default value
	// in the usage display. This will add it as the config, then if a token flag
	// is provided too, the flag will take precedence.
	cfg := getConfigFromDefaultPath()

	// we have some indirection here b/c of how the Config is embedded on the
	// global flags type. For the time being, we check to see if there was a
	// value set on flags registered (via env vars), and override the host/token
	// values if they are.
	if flags.Token != "" {
		cfg.Token = flags.Token
	}
	if flags.Host != "" {
		cfg.Host = flags.Host
	}
	flags.Config = cfg

	cmd.PersistentFlags().BoolVar(&flags.skipVerify, "skip-verify", false, "SkipVerify controls whether a client verifies the server's certificate chain and host name.")

	// Update help description for all commands in command tree
	walk(cmd, func(c *cobra.Command) {
		c.Flags().BoolP("help", "h", false, fmt.Sprintf("Help for the %s command ", c.Name()))
	})

	// completion command goes last, after the walk, so that all
	// commands have every flag listed in the bash|zsh completions.
	cmd.AddCommand(
		completionCmd(cmd),
		cmdVersion(),
	)
	return cmd
}

func cmdVersion() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the influx CLI version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Influx CLI %s (git: %s) build_date: %s\n", version, commit, date)
		},
	}
}

func influxCmd(opts ...genericCLIOptFn) *cobra.Command {
	builder := newInfluxCmdBuilder(opts...)
	return builder.cmd(
		cmdAuth,
		cmdBackup,
		cmdBucket,
		cmdConfig,
		cmdDelete,
		cmdExport,
		cmdOrganization,
		cmdPing,
		cmdQuery,
		cmdREPL,
		cmdSecret,
		cmdSetup,
		cmdStack,
		cmdTask,
		cmdTemplate,
		cmdApply,
		cmdTranspile,
		cmdUser,
		cmdWrite,
	)
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

func defaultConfigPath() (string, string, error) {
	dir, err := fs.InfluxDir()
	if err != nil {
		return "", "", err
	}
	return filepath.Join(dir, http.DefaultConfigsFile), dir, nil
}

func getConfigFromDefaultPath() config.Config {
	path, _, err := defaultConfigPath()
	if err != nil {
		return config.DefaultConfig
	}
	r, err := os.Open(path)
	if err != nil {
		return config.DefaultConfig
	}
	activated, err := config.ParseActiveConfig(r)
	if err != nil {
		return config.DefaultConfig
	}
	return activated
}

func migrateOldCredential() {
	dir, err := fs.InfluxDir()
	if err != nil {
		return // no need for migration
	}

	tokB, err := ioutil.ReadFile(filepath.Join(dir, http.DefaultTokenFile))
	if err != nil {
		return // no need for migration
	}

	err = writeConfigToPath(strings.TrimSpace(string(tokB)), "", filepath.Join(dir, http.DefaultConfigsFile), dir)
	if err != nil {
		return
	}

	// ignore the remove err
	_ = os.Remove(filepath.Join(dir, http.DefaultTokenFile))
}

func writeConfigToPath(tok, org, path, dir string) error {
	p := &config.DefaultConfig
	p.Token = tok
	p.Org = org

	_, err := config.NewLocalConfigSVC(path, dir).CreateConfig(*p)
	return err
}

func checkSetup(host string, skipVerify bool) error {
	s := &http.SetupService{
		Addr:               host,
		InsecureSkipVerify: skipVerify,
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

func checkSetupRunEMiddleware(f *globalFlags) cobraRunEMiddleware {
	return func(fn cobraRunEFn) cobraRunEFn {
		return func(cmd *cobra.Command, args []string) error {
			err := fn(cmd, args)
			if err == nil {
				return nil
			}

			if setupErr := checkSetup(f.Host, f.skipVerify); setupErr != nil && influxdb.EUnauthorized != influxdb.ErrorCode(setupErr) {
				cmd.OutOrStderr().Write([]byte(fmt.Sprintf("Error: %s\n", internal.ErrorFmt(err).Error())))
				return internal.ErrorFmt(setupErr)
			}

			return internal.ErrorFmt(err)
		}
	}
}

// walk calls f for c and all of its children.
func walk(c *cobra.Command, f func(*cobra.Command)) {
	f(c)
	for _, c := range c.Commands() {
		walk(c, f)
	}
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
	}

	getOrgByName := func(name string) (influxdb.ID, error) {
		org, err := orgSVC.FindOrganization(context.Background(), influxdb.OrganizationFilter{
			Name: &name,
		})
		if err != nil {
			return 0, err
		}
		return org.ID, nil
	}
	if o.name != "" {
		return getOrgByName(o.name)
	}
	// last check is for the org set in the CLI config. This will be last in priority.
	if flags.Org != "" {
		return getOrgByName(flags.Org)
	}
	return 0, fmt.Errorf("failed to locate organization criteria")
}

func (o *organization) validOrgFlags(f *globalFlags) error {
	if o.id == "" && o.name == "" && f != nil {
		o.name = f.Org
	}

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

func registerPrintOptions(cmd *cobra.Command, headersP, jsonOutP *bool) {
	var opts flagOpts
	if headersP != nil {
		opts = append(opts, cli.Opt{
			DestP:   headersP,
			Flag:    "hide-headers",
			EnvVar:  "HIDE_HEADERS",
			Desc:    "Hide the table headers; defaults false",
			Default: false,
		})
	}
	if jsonOutP != nil {
		opts = append(opts, cli.Opt{
			DestP:   jsonOutP,
			Flag:    "json",
			EnvVar:  "OUTPUT_JSON",
			Desc:    "Output data as json; defaults false",
			Default: false,
		})
	}
	opts.mustRegister(cmd)
}

func setViperOptions() {
	viper.SetEnvPrefix("INFLUX")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
}

func writeJSON(w io.Writer, v interface{}) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "\t")
	return enc.Encode(v)
}

func newBucketService() (influxdb.BucketService, error) {
	client, err := newHTTPClient()
	if err != nil {
		return nil, err
	}

	return &http.BucketService{
		Client: client,
	}, nil
}
