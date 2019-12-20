package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/cmd/influx/internal"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/internal/fs"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

const maxTCPConnections = 128

func main() {
	influxCmd := influxCmd()
	if err := influxCmd.Execute(); err != nil {
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

	viper.SetEnvPrefix("INFLUX")

	cmd.AddCommand(
		authCmd(),
		bucketCmd,
		deleteCmd,
		organizationCmd(),
		pingCmd,
		cmdPkg(newPkgerSVC),
		queryCmd,
		transpileCmd,
		replCmd,
		setupCmd,
		taskCmd,
		userCmd(),
		writeCmd,
	)

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

	// Update help description for all commands in command tree
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

	store := bolt.NewKVStore(zap.NewNop(), boltFile)
	if err := store.Open(context.Background()); err != nil {
		return nil, err
	}

	return kv.NewService(zap.NewNop(), store), nil
}

type organization struct {
	id, name string
}

func (org *organization) register(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&org.id, "org-id", "", "", "The ID of the organization that owns the bucket")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		org.id = h
	}
	cmd.Flags().StringVarP(&org.name, "org", "o", "", "The name of the organization that owns the bucket")
	viper.BindEnv("ORG")
	if h := viper.GetString("ORG"); h != "" {
		org.name = h
	}
}

func (org *organization) getID(orgSVC influxdb.OrganizationService) (influxdb.ID, error) {
	if org.id != "" {
		influxOrgID, err := influxdb.IDFromString(org.id)
		if err != nil {
			return 0, fmt.Errorf("invalid org ID provided: %s", err.Error())
		}
		return *influxOrgID, nil
	} else if org.name != "" {
		org, err := orgSVC.FindOrganization(context.Background(), influxdb.OrganizationFilter{
			Name: &org.name,
		})
		if err != nil {
			return 0, fmt.Errorf("%v", err)
		}
		return org.ID, nil
	}
	return 0, fmt.Errorf("failed to locate an organization id")
}

func (org *organization) validOrgFlags() error {
	if org.id == "" && org.name == "" {
		return fmt.Errorf("must specify org-id, or org name")
	} else if org.id != "" && org.name != "" {
		return fmt.Errorf("must specify org-id, or org name not both")
	}
	return nil
}
