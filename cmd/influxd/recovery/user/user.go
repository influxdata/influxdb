package user

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

func NewUserCommand() *cobra.Command {
	base := &cobra.Command{
		Use:   "user",
		Short: "On-disk user management commands, for recovery",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			cmd.PrintErrf("See '%s -h' for help\n", cmd.CommandPath())
		},
	}

	base.AddCommand(NewUserListCommand())
	base.AddCommand(NewUserCreateCommand())

	return base
}

type userListCommand struct {
	logger   *zap.Logger
	boltPath string
	out      io.Writer
}

func NewUserListCommand() *cobra.Command {
	var userCmd userListCommand
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List users",
		RunE: func(cmd *cobra.Command, args []string) error {
			config := logger.NewConfig()
			config.Level = zapcore.InfoLevel

			newLogger, err := config.New(cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			userCmd.logger = newLogger
			userCmd.out = cmd.OutOrStdout()
			return userCmd.run()
		},
	}

	defaultPath := filepath.Join(os.Getenv("HOME"), ".influxdbv2", "influxd.bolt")

	cmd.Flags().StringVar(&userCmd.boltPath, "bolt-path", defaultPath, "Path to the TSM data directory.")

	return cmd
}

func (cmd *userListCommand) run() error {
	ctx := context.Background()
	store := bolt.NewKVStore(cmd.logger.With(zap.String("system", "bolt-kvstore")), cmd.boltPath)
	if err := store.Open(ctx); err != nil {
		return err
	}
	defer store.Close()
	tenantStore := tenant.NewStore(store)
	tenantService := tenant.NewService(tenantStore)
	filter := influxdb.UserFilter{}
	users, _, err := tenantService.FindUsers(ctx, filter)
	if err != nil {
		return err
	}

	return PrintUsers(ctx, cmd.out, users)
}

type userCreateCommand struct {
	logger   *zap.Logger
	boltPath string
	out      io.Writer
	username string
}

func NewUserCreateCommand() *cobra.Command {
	var userCmd userCreateCommand
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create new user",
		RunE: func(cmd *cobra.Command, args []string) error {
			config := logger.NewConfig()
			config.Level = zapcore.InfoLevel

			newLogger, err := config.New(cmd.ErrOrStderr())
			if err != nil {
				return err
			}
			userCmd.logger = newLogger
			userCmd.out = cmd.OutOrStdout()
			return userCmd.run()
		},
	}

	defaultPath := filepath.Join(os.Getenv("HOME"), ".influxdbv2", "influxd.bolt")
	cmd.Flags().StringVar(&userCmd.boltPath, "bolt-path", defaultPath, "Path to the TSM data directory")
	cmd.Flags().StringVar(&userCmd.username, "username", "", "Name of the user")

	return cmd
}

func (cmd *userCreateCommand) run() error {
	ctx := context.Background()
	store := bolt.NewKVStore(cmd.logger.With(zap.String("system", "bolt-kvstore")), cmd.boltPath)
	if err := store.Open(ctx); err != nil {
		return err
	}
	defer store.Close()
	tenantStore := tenant.NewStore(store)
	tenantService := tenant.NewService(tenantStore)

	if cmd.username == "" {
		return fmt.Errorf("must provide --username")
	}

	// Find the user
	user, err := tenantService.FindUser(ctx, influxdb.UserFilter{Name: &cmd.username})
	if err == nil || user != nil {
		return fmt.Errorf("cannot create existing user %s", cmd.username)
	}

	if err := tenantService.CreateUser(ctx, &influxdb.User{
		Name: cmd.username,
	}); err != nil {
		return err
	}

	// Print all users now that we have added one
	filter := influxdb.UserFilter{}
	users, _, err := tenantService.FindUsers(ctx, filter)
	if err != nil {
		return err
	}
	return PrintUsers(ctx, cmd.out, users)
}

func PrintUsers(ctx context.Context, w io.Writer, v []*influxdb.User) error {
	headers := []string{"ID", "Name"}

	var rows []map[string]interface{}
	for _, u := range v {
		row := map[string]interface{}{
			"ID":   u.ID,
			"Name": u.Name,
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
