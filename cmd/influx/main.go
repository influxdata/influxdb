package main

import (
	"context"
	"fmt"
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

var influxCmd = &cobra.Command{
	Use:   "influx",
	Short: "Influx Client",
	Run: func(cmd *cobra.Command, args []string) {
		if err := checkSetup(flags.host); err != nil {
			fmt.Printf("Note: %v\n", internal.ErrorFmt(err))
		}
		cmd.Usage()
	},
}

const maxTCPConnections = 128

func init() {
	influxCmd.AddCommand(
		authorizationCmd,
		bucketCmd,
		deleteCmd,
		organizationCmd,
		pingCmd,
		pkgCmd(),
		queryCmd,
		replCmd,
		setupCmd,
		taskCmd,
		userCmd,
		writeCmd,
	)

	viper.SetEnvPrefix("INFLUX")

	influxCmd.PersistentFlags().StringVarP(&flags.token, "token", "t", "", "API token to be used throughout client calls")
	viper.BindEnv("TOKEN")
	if h := viper.GetString("TOKEN"); h != "" {
		flags.token = h
	} else if tok, err := getTokenFromDefaultPath(); err == nil {
		flags.token = tok
	}

	influxCmd.PersistentFlags().StringVar(&flags.host, "host", "http://localhost:9999", "HTTP address of Influx")
	viper.BindEnv("HOST")
	if h := viper.GetString("HOST"); h != "" {
		flags.host = h
	}

	influxCmd.PersistentFlags().BoolVar(&flags.local, "local", false, "Run commands locally against the filesystem")

	// Override help on all the commands tree
	walk(influxCmd, func(c *cobra.Command) {
		c.Flags().BoolP("help", "h", false, fmt.Sprintf("Help for the %s command ", c.Name()))
	})
}

func main() {
	if err := influxCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// Flags contains all the CLI flag values for influx.
type Flags struct {
	token string
	host  string
	local bool
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
		Addr: flags.host,
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
