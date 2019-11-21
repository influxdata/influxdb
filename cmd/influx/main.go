package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/cmd/influx/internal"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/internal/fs"
	"github.com/influxdata/influxdb/kv"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const maxTCPConnections = 128

func main() {
	influxCmd := influxCmd()
	if err := influxCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

type httpClientOpts struct {
	token, addr string
	skipVerify  bool
}

type genericCLIOptfn func(*genericCLIOpts)

type genericCLIOpts struct {
	in io.Reader
	w  io.Writer
}

func (o genericCLIOpts) newCmd(use string) *cobra.Command {
	cmd := &cobra.Command{Use: use}
	cmd.SetOutput(o.w)
	return cmd
}

func in(r io.Reader) genericCLIOptfn {
	return func(o *genericCLIOpts) {
		o.in = r
	}
}

func out(w io.Writer) genericCLIOptfn {
	return func(o *genericCLIOpts) {
		o.w = w
	}
}

func influxCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "influx",
		Short: "Influx Client",
		Run: func(cmd *cobra.Command, args []string) {
			if err := checkSetup(flags.host); err != nil {
				fmt.Printf("Note: %v\n", internal.ErrorFmt(err))
			}
			cmd.Usage()
		},
	}
	cmd.AddCommand(
		authCmd(),
		bucketCmd,
		deleteCmd,
		organizationCmd,
		pingCmd,
		cmdPkg(newPkgerSVC),
		queryCmd,
		replCmd,
		setupCmd,
		taskCmd,
		userCmd(),
		writeCmd,
	)

	viper.SetEnvPrefix("INFLUX")

	cmd.PersistentFlags().StringVarP(&flags.token, "token", "t", "", "API token to be used throughout client calls")
	viper.BindEnv("TOKEN")
	if h := viper.GetString("TOKEN"); h != "" {
		flags.token = h
	} else if tok, err := getTokenFromDefaultPath(); err == nil {
		flags.token = tok
	}

	cmd.PersistentFlags().StringVar(&flags.host, "host", "http://localhost:9999", "HTTP address of Influx")
	viper.BindEnv("HOST")
	if h := viper.GetString("HOST"); h != "" {
		flags.host = h
	}

	cmd.PersistentFlags().BoolVar(&flags.local, "local", false, "Run commands locally against the filesystem")

	cmd.PersistentFlags().BoolVar(&flags.skipVerify, "skip-verify", false, "SkipVerify controls whether a client verifies the server's certificate chain and host name.")

	// Override help on all the commands tree
	walk(cmd, func(c *cobra.Command) {
		c.Flags().BoolP("help", "h", false, fmt.Sprintf("Help for the %s command ", c.Name()))
	})

	return cmd
}

// Flags contains all the CLI flag values for influx.
type Flags struct {
	token      string
	host       string
	local      bool
	skipVerify bool
}

func (f Flags) httpClientOpts() httpClientOpts {
	return httpClientOpts{
		addr:       f.host,
		token:      f.token,
		skipVerify: f.skipVerify,
	}
}

var flags Flags

func defaultTokenPath() (string, string, error) {
	dir, err := fs.InfluxDir()
	if err != nil {
		return "", "", err
	}
	return filepath.Join(dir, "credentials"), dir, nil
}

func getTokenFromDefaultPath() (string, error) {
	path, _, err := defaultTokenPath()
	if err != nil {
		return "", err
	}
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(b), nil
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

	store := bolt.NewKVStore(boltFile)
	if err := store.Open(context.Background()); err != nil {
		return nil, err
	}

	return kv.NewService(store), nil
}
