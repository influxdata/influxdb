package main

import (
	"context"
	"fmt"
	"os"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/bolt"
	"github.com/influxdata/platform/cmd/influx/internal"
	"github.com/influxdata/platform/http"
	"github.com/influxdata/platform/internal/fs"
	"github.com/spf13/cobra"
)

var userCmd = &cobra.Command{
	Use:   "user",
	Short: "User management commands",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Usage()
	},
}

// UserUpdateFlags are command line args used when updating a user
type UserUpdateFlags struct {
	id   string
	name string
}

var userUpdateFlags UserUpdateFlags

func init() {
	userUpdateCmd := &cobra.Command{
		Use:   "update",
		Short: "Update user",
		Run:   userUpdateF,
	}

	userUpdateCmd.Flags().StringVarP(&userUpdateFlags.id, "id", "i", "", "user id (required)")
	userUpdateCmd.Flags().StringVarP(&userUpdateFlags.name, "name", "n", "", "user name")
	userUpdateCmd.MarkFlagRequired("id")

	userCmd.AddCommand(userUpdateCmd)
}

func newUserService(f Flags) (platform.UserService, error) {
	if flags.local {
		boltFile, err := fs.BoltFile()
		if err != nil {
			return nil, err
		}
		c := bolt.NewClient()
		c.Path = boltFile
		if err := c.Open(context.Background()); err != nil {
			return nil, err
		}

		return c, nil
	}
	return &http.UserService{
		Addr:  flags.host,
		Token: flags.token,
	}, nil
}

func newUserResourceMappingService(f Flags) (platform.UserResourceMappingService, error) {
	if flags.local {
		boltFile, err := fs.BoltFile()
		if err != nil {
			return nil, err
		}
		c := bolt.NewClient()
		c.Path = boltFile
		if err := c.Open(context.Background()); err != nil {
			return nil, err
		}

		return c, nil
	}
	return &http.UserResourceMappingService{
		Addr:  flags.host,
		Token: flags.token,
	}, nil
}

func userUpdateF(cmd *cobra.Command, args []string) {
	s, err := newUserService(flags)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var id platform.ID
	if err := id.DecodeFromString(userUpdateFlags.id); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	update := platform.UserUpdate{}
	if userUpdateFlags.name != "" {
		update.Name = &userUpdateFlags.name
	}

	user, err := s.UpdateUser(context.Background(), id, update)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
	)
	w.Write(map[string]interface{}{
		"ID":   user.ID.String(),
		"Name": user.Name,
	})
	w.Flush()
}

// UserCreateFlags are command line args used when creating a user
type UserCreateFlags struct {
	name string
}

var userCreateFlags UserCreateFlags

func init() {
	userCreateCmd := &cobra.Command{
		Use:   "create",
		Short: "Create user",
		Run:   userCreateF,
	}

	userCreateCmd.Flags().StringVarP(&userCreateFlags.name, "name", "n", "", "user name (required)")
	userCreateCmd.MarkFlagRequired("name")

	userCmd.AddCommand(userCreateCmd)
}

func userCreateF(cmd *cobra.Command, args []string) {
	s, err := newUserService(flags)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	user := &platform.User{
		Name: userCreateFlags.name,
	}

	if err := s.CreateUser(context.Background(), user); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
	)
	w.Write(map[string]interface{}{
		"ID":   user.ID.String(),
		"Name": user.Name,
	})
	w.Flush()
}

// UserFindFlags are command line args used when finding a user
type UserFindFlags struct {
	id   string
	name string
}

var userFindFlags UserFindFlags

func init() {
	userFindCmd := &cobra.Command{
		Use:   "find",
		Short: "Find user",
		Run:   userFindF,
	}

	userFindCmd.Flags().StringVarP(&userFindFlags.id, "id", "i", "", "user ID")
	userFindCmd.Flags().StringVarP(&userFindFlags.name, "name", "n", "", "user name")

	userCmd.AddCommand(userFindCmd)
}

func userFindF(cmd *cobra.Command, args []string) {
	s, err := newUserService(flags)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	filter := platform.UserFilter{}
	if userFindFlags.name != "" {
		filter.Name = &userFindFlags.name
	}
	if userFindFlags.id != "" {
		id, err := platform.IDFromString(userFindFlags.id)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		filter.ID = id
	}

	users, _, err := s.FindUsers(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
	)
	for _, u := range users {
		w.Write(map[string]interface{}{
			"ID":   u.ID.String(),
			"Name": u.Name,
		})
	}
	w.Flush()
}

// UserDeleteFlags are command line args used when deleting a user
type UserDeleteFlags struct {
	id string
}

var userDeleteFlags UserDeleteFlags

func init() {
	userDeleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete user",
		Run:   userDeleteF,
	}

	userDeleteCmd.Flags().StringVarP(&userDeleteFlags.id, "id", "i", "", "user id (required)")
	userDeleteCmd.MarkFlagRequired("id")

	userCmd.AddCommand(userDeleteCmd)
}

func userDeleteF(cmd *cobra.Command, args []string) {
	s, err := newUserService(flags)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var id platform.ID
	if err := id.DecodeFromString(userDeleteFlags.id); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ctx := context.TODO()
	u, err := s.FindUserByID(ctx, id)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := s.DeleteUser(ctx, id); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
		"Deleted",
	)
	w.Write(map[string]interface{}{
		"ID":      u.ID.String(),
		"Name":    u.Name,
		"Deleted": true,
	})
	w.Flush()
}
