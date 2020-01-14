package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influx/internal"
	"github.com/influxdata/influxdb/http"
	"github.com/spf13/cobra"
	input "github.com/tcnksm/go-input"
)

func cmdUser() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "user",
		Short: "User management commands",
		Run:   seeHelp,
	}
	cmd.AddCommand(
		userCreateCmd(),
		userDeleteCmd(),
		userFindCmd(),
		userUpdateCmd(),
		userUpdatePasswordCmd(),
	)

	return cmd
}

var userUpdateFlags struct {
	id   string
	name string
}

func userUpdatePasswordCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "password",
		Short: "Update user password",
		RunE:  wrapCheckSetup(userUpdatePasswordF),
	}

	cmd.Flags().StringVarP(&userFindFlags.id, "id", "i", "", "The user ID")
	cmd.Flags().StringVarP(&userFindFlags.name, "name", "n", "", "The user name")

	return cmd
}

func userUpdateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "Update user",
		RunE:  wrapCheckSetup(userUpdateF),
	}

	cmd.Flags().StringVarP(&userUpdateFlags.id, "id", "i", "", "The user ID (required)")
	cmd.Flags().StringVarP(&userUpdateFlags.name, "name", "n", "", "The user name")
	cmd.MarkFlagRequired("id")

	return cmd
}

func newPasswordService() (platform.PasswordsService, error) {
	if flags.local {
		return newLocalKVService()
	}

	client, err := newHTTPClient()
	if err != nil {
		return nil, err
	}
	return &http.PasswordService{
		Client: client,
	}, nil
}

func newUserService() (platform.UserService, error) {
	if flags.local {
		return newLocalKVService()
	}

	client, err := newHTTPClient()
	if err != nil {
		return nil, err
	}
	return &http.UserService{
		Client: client,
	}, nil
}

func userUpdatePasswordF(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	us, err := newUserService()
	if err != nil {
		return err
	}

	filter := platform.UserFilter{}
	if userFindFlags.name != "" {
		filter.Name = &userFindFlags.name
	}
	if userFindFlags.id != "" {
		id, err := platform.IDFromString(userFindFlags.id)
		if err != nil {
			return err
		}
		filter.ID = id
	}
	u, err := us.FindUser(ctx, filter)
	if err != nil {
		return err
	}
	ui := &input.UI{
		Writer: os.Stdout,
		Reader: os.Stdin,
	}
	password := getPassword(ui, true)

	ps, err := newPasswordService()
	if err != nil {
		return err
	}
	if err = ps.SetPassword(ctx, u.ID, password); err != nil {
		return err
	}
	fmt.Println("Your password has been successfully updated.")
	return nil
}

func userUpdateF(cmd *cobra.Command, args []string) error {
	s, err := newUserService()
	if err != nil {
		return err
	}

	var id platform.ID
	if err := id.DecodeFromString(userUpdateFlags.id); err != nil {
		return err
	}

	update := platform.UserUpdate{}
	if userUpdateFlags.name != "" {
		update.Name = &userUpdateFlags.name
	}

	user, err := s.UpdateUser(context.Background(), id, update)
	if err != nil {
		return err
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

	return nil
}

var userCreateFlags struct {
	name     string
	password string
	org      organization
}

func userCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create user",
		RunE:  wrapCheckSetup(userCreateF),
	}

	userCreateFlags.org.register(cmd, false)
	cmd.Flags().StringVarP(&userCreateFlags.name, "name", "n", "", "The user name (required)")
	cmd.MarkFlagRequired("name")
	cmd.Flags().StringVarP(&userCreateFlags.password, "password", "p", "", "The user password")

	return cmd
}

func userCreateF(cmd *cobra.Command, args []string) error {
	if err := userCreateFlags.org.validOrgFlags(); err != nil {
		return err
	}

	s, err := newUserService()
	if err != nil {
		return err
	}

	user := &platform.User{
		Name: userCreateFlags.name,
	}

	if err := s.CreateUser(context.Background(), user); err != nil {
		return err
	}

	writeOutput := func(headers []string, vals ...string) error {
		if len(headers) != len(vals) {
			return errors.New("invalid headers and val setup for writer")
		}

		m := make(map[string]interface{})
		for i, h := range headers {
			m[h] = vals[i]
		}
		w := internal.NewTabWriter(os.Stdout)
		w.WriteHeaders(headers...)
		w.Write(m)
		w.Flush()

		return nil
	}

	orgSVC, err := newOrganizationService()
	if err != nil {
		return err
	}

	orgID, err := userCreateFlags.org.getID(orgSVC)
	if err != nil {
		return err
	}

	pass := userCreateFlags.password
	if orgID == 0 && pass == "" {
		return writeOutput([]string{"ID", "Name"}, user.ID.String(), user.Name)
	}

	if pass != "" && orgID == 0 {
		return errors.New("an org id is required when providing a user password")
	}

	c, err := newHTTPClient()
	if err != nil {
		return err
	}

	userResMapSVC := &http.UserResourceMappingService{
		Client: c,
	}

	err = userResMapSVC.CreateUserResourceMapping(context.Background(), &platform.UserResourceMapping{
		UserID:       user.ID,
		UserType:     platform.Member,
		ResourceType: platform.OrgsResourceType,
		ResourceID:   orgID,
	})
	if err != nil {
		return err
	}

	passSVC := &http.PasswordService{Client: c}

	ctx := context.Background()
	if err := passSVC.SetPassword(ctx, user.ID, pass); err != nil {
		return err
	}

	return writeOutput([]string{"ID", "Name", "Organization ID"}, user.ID.String(), user.Name, orgID.String())
}

var userFindFlags struct {
	id   string
	name string
}

func userFindCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "find",
		Short: "Find user",
		RunE:  wrapCheckSetup(userFindF),
	}

	cmd.Flags().StringVarP(&userFindFlags.id, "id", "i", "", "The user ID")
	cmd.Flags().StringVarP(&userFindFlags.name, "name", "n", "", "The user name")

	return cmd
}

func userFindF(cmd *cobra.Command, args []string) error {
	s, err := newUserService()
	if err != nil {
		return err
	}

	filter := platform.UserFilter{}
	if userFindFlags.name != "" {
		filter.Name = &userFindFlags.name
	}
	if userFindFlags.id != "" {
		id, err := platform.IDFromString(userFindFlags.id)
		if err != nil {
			return err
		}
		filter.ID = id
	}

	users, _, err := s.FindUsers(context.Background(), filter)
	if err != nil {
		return err
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

	return nil
}

var userDeleteFlags struct {
	id string
}

func userDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete user",
		RunE:  wrapCheckSetup(userDeleteF),
	}

	cmd.Flags().StringVarP(&userDeleteFlags.id, "id", "i", "", "The user ID (required)")
	cmd.MarkFlagRequired("id")

	return cmd
}

func userDeleteF(cmd *cobra.Command, args []string) error {
	s, err := newUserService()
	if err != nil {
		return err
	}

	var id platform.ID
	if err := id.DecodeFromString(userDeleteFlags.id); err != nil {
		return err
	}

	ctx := context.Background()
	u, err := s.FindUserByID(ctx, id)
	if err != nil {
		return err
	}

	if err := s.DeleteUser(ctx, id); err != nil {
		return err
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

	return nil
}
