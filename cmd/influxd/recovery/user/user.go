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

	cmd.Flags().StringVar(&userCmd.boltPath, "bolt-path", defaultPath, "Path to the BoltDB file.")

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
	password string
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
	cmd.Flags().StringVar(&userCmd.boltPath, "bolt-path", defaultPath, "Path to the BoltDB file")
	cmd.Flags().StringVar(&userCmd.username, "username", "", "Name of the user")
	cmd.Flags().StringVar(&userCmd.password, "password", "", "Password for new user")

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
	if cmd.password == "" {
		return fmt.Errorf("must provide --password")
	}

	user := influxdb.User{
		Name: cmd.username,
	}

	if err := tenantService.CreateUser(ctx, &user); err != nil {
		return err
	}

	if err := tenantService.SetPassword(ctx, user.ID, cmd.password); err != nil {
		// attempt to delete new user because password failed
		if delErr := tenantService.DeleteUser(ctx, user.ID); delErr != nil {
			fmt.Fprintf(cmd.out, "Could not delete bad user %q after password set failed: %s", cmd.username, delErr)
		}
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
