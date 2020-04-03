package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/influxdata/flux"
	_ "github.com/influxdata/flux/stdlib"
	_ "github.com/influxdata/influxdb/query/stdlib"
	"github.com/spf13/cobra"
)

var queryFlags struct {
	org  organization
	file string
}

func cmdQuery(f *globalFlags, opts genericCLIOpts) *cobra.Command {
	cmd := opts.newCmd("query [query literal or -f /path/to/query.flux]", fluxQueryF, true)
	cmd.Short = "Execute a Flux query"
	cmd.Long = `Execute a literal Flux query provided as an argument or 
in a file using the -f flag or via stdin`
	cmd.Args = cobra.MaximumNArgs(1)

	queryFlags.org.register(cmd, true)
	cmd.Flags().StringVarP(&queryFlags.file, "file", "f", "", "The path to the Flux query file")

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
	if flags.local {
		return fmt.Errorf("local flag not supported for query command")
	}

	if err := queryFlags.org.validOrgFlags(&flags); err != nil {
		return err
	}

	q, err := readFluxQuery(args, queryFlags.file)
	if err != nil {
		return fmt.Errorf("failed to load query: %v", err)
	}

	orgSvc, err := newOrganizationService()
	if err != nil {
		return fmt.Errorf("failed to initialized organization service client: %v", err)
	}

	orgID, err := queryFlags.org.getID(orgSvc)
	if err != nil {
		return err
	}

	flux.FinalizeBuiltIns()

	r, err := getFluxREPL(flags.Host, flags.Token, flags.skipVerify, orgID)
	if err != nil {
		return fmt.Errorf("failed to get the flux REPL: %v", err)
	}

	if err := r.Input(q); err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}

	return nil
}
