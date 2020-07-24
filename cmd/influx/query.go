package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/dependencies/filesystem"
	"github.com/influxdata/flux/plan"
	"github.com/influxdata/flux/repl"
	"github.com/influxdata/flux/runtime"
	_ "github.com/influxdata/flux/stdlib"
	"github.com/influxdata/flux/stdlib/influxdata/influxdb"
	_ "github.com/influxdata/influxdb/v2/query/stdlib"
	"github.com/spf13/cobra"
)

var queryFlags struct {
	org  organization
	file string
}

func cmdQuery(f *globalFlags, opts genericCLIOpts) *cobra.Command {
	cmd := opts.newCmd("query [query literal or -f /path/to/query.flux]", fluxQueryF, true)
	cmd.Short = "Execute a Flux query"
	cmd.Long = `Execute a Flux query provided via the first argument or a file or stdin`
	cmd.Args = cobra.MaximumNArgs(1)

	f.registerFlags(cmd)
	queryFlags.org.register(cmd, true)
	cmd.Flags().StringVarP(&queryFlags.file, "file", "f", "", "Path to Flux query file")

	return cmd
}

// readFluxQuery returns first argument, file contents or stdin
func readFluxQuery(args []string, file string) (string, error) {
	// backward compatibility
	if len(args) > 0 {
		if strings.HasPrefix(args[0], "@") {
			file = args[0][1:]
			args = args[:0]
		} else if args[0] == "-" {
			file = ""
			args = args[:0]
		}
	}

	var query string
	switch {
	case len(args) > 0:
		query = args[0]
	case len(file) > 0:
		content, err := ioutil.ReadFile(file)
		if err != nil {
			return "", err
		}
		query = string(content)
	default:
		content, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return "", err
		}
		query = string(content)
	}
	return query, nil
}

func fluxQueryF(cmd *cobra.Command, args []string) error {
	if err := queryFlags.org.validOrgFlags(&flags); err != nil {
		return err
	}

	q, err := readFluxQuery(args, queryFlags.file)
	if err != nil {
		return fmt.Errorf("failed to load query: %v", err)
	}

	plan.RegisterLogicalRules(
		influxdb.DefaultFromAttributes{
			Org: &influxdb.NameOrID{
				ID:   queryFlags.org.id,
				Name: queryFlags.org.name,
			},
			Host:  &flags.Host,
			Token: &flags.Token,
		},
	)
	runtime.FinalizeBuiltIns()

	r, err := getFluxREPL(flags.skipVerify)
	if err != nil {
		return fmt.Errorf("failed to get the flux REPL: %v", err)
	}

	if err := r.Input(q); err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}

	return nil
}

func getFluxREPL(skipVerify bool) (*repl.REPL, error) {
	deps := flux.NewDefaultDependencies()
	deps.Deps.FilesystemService = filesystem.SystemFS
	if skipVerify {
		deps.Deps.HTTPClient = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}
	}
	ctx := deps.Inject(context.Background())
	return repl.New(ctx, deps), nil
}
