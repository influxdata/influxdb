package organization

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/internal/tabwriter"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewOrgCommand() *cobra.Command {
	base := &cobra.Command{
		Use:   "org",
		Short: "On-disk organization management commands, for recovery",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cmd.PrintErrf("See '%s -h' for help\n", cmd.CommandPath())
		},
	}

	base.AddCommand(NewOrgListCommand())
	base.AddCommand(NewOrgCreateCommand())

	return base
}

type orgListCommand struct {
	logger   *zap.Logger
	boltPath string
	out      io.Writer
}

func NewOrgListCommand() *cobra.Command {
	var orgCmd orgListCommand
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List organizations",
		RunE: func(cmd *cobra.Command, args []string) error {
			config := logger.NewConfig()
			config.Level = zapcore.InfoLevel

			newLogger, err := config.New(cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			orgCmd.logger = newLogger
			orgCmd.out = cmd.OutOrStdout()
			return orgCmd.run()
		},
	}

	defaultPath := filepath.Join(os.Getenv("HOME"), ".influxdbv2", "influxd.bolt")

	cmd.Flags().StringVar(&orgCmd.boltPath, "bolt-path", defaultPath, "Path to the BoltDB file")

	return cmd
}

func (cmd *orgListCommand) run() error {
	ctx := context.Background()
	store := bolt.NewKVStore(cmd.logger.With(zap.String("system", "bolt-kvstore")), cmd.boltPath)
	if err := store.Open(ctx); err != nil {
		return err
	}
	defer store.Close()
	tenantStore := tenant.NewStore(store)
	tenantService := tenant.NewService(tenantStore)
	orgs, _, err := tenantService.FindOrganizations(ctx, influxdb.OrganizationFilter{})
	if err != nil {
		return err
	}

	return PrintOrgs(ctx, cmd.out, orgs)
}

type orgCreateCommand struct {
	logger   *zap.Logger
	boltPath string
	out      io.Writer
	org      string
}

func NewOrgCreateCommand() *cobra.Command {
	var orgCmd orgCreateCommand
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create new org",
		RunE: func(cmd *cobra.Command, args []string) error {
			config := logger.NewConfig()
			config.Level = zapcore.InfoLevel

			newLogger, err := config.New(cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			orgCmd.logger = newLogger
			orgCmd.out = cmd.OutOrStdout()
			return orgCmd.run()
		},
	}

	defaultPath := filepath.Join(os.Getenv("HOME"), ".influxdbv2", "influxd.bolt")
	cmd.Flags().StringVar(&orgCmd.boltPath, "bolt-path", defaultPath, "Path to the BoltDB file")
	cmd.Flags().StringVar(&orgCmd.org, "org", "", "Name of the org to create")

	return cmd
}

func (cmd *orgCreateCommand) run() error {
	ctx := context.Background()
	store := bolt.NewKVStore(cmd.logger.With(zap.String("system", "bolt-kvstore")), cmd.boltPath)
	if err := store.Open(ctx); err != nil {
		return err
	}
	defer store.Close()
	tenantStore := tenant.NewStore(store)
	tenantService := tenant.NewService(tenantStore)
	if cmd.org == "" {
		return fmt.Errorf("must provide --org")
	}

	if err := tenantService.CreateOrganization(ctx, &influxdb.Organization{
		Name: cmd.org,
	}); err != nil {
		return err
	}

	orgs, _, err := tenantService.FindOrganizations(ctx, influxdb.OrganizationFilter{})
	if err != nil {
		return err
	}
	return PrintOrgs(ctx, cmd.out, orgs)
}

func PrintOrgs(ctx context.Context, w io.Writer, v []*influxdb.Organization) error {
	headers := []string{
		"ID",
		"Name",
	}

	var rows []map[string]interface{}
	for _, org := range v {
		row := map[string]interface{}{
			"ID":   org.ID,
			"Name": org.Name,
		}
		rows = append(rows, row)
	}

	writer := tabwriter.NewTabWriter(w, false)
	defer writer.Flush()
	if err := writer.WriteHeaders(headers...); err != nil {
		return err
	}
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	return nil
}
