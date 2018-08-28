package main

import (
	"context"
	"fmt"
	"os"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/cmd/influx/internal"
	"github.com/influxdata/platform/http"
	"github.com/spf13/cobra"
)

var authorizationCmd = &cobra.Command{
	Use:     "auth",
	Aliases: []string{"authorization"},
	Short:   "Authorization management commands",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Usage()
	},
}

// AuthorizationCreateFlags are command line args used when creating a authorization
type AuthorizationCreateFlags struct {
	user string

	createUserPermission bool
	deleteUserPermission bool

	readBucketPermissions  []string
	writeBucketPermissions []string
}

var authorizationCreateFlags AuthorizationCreateFlags

func init() {
	authorizationCreateCmd := &cobra.Command{
		Use:   "create",
		Short: "Create authorization",
		Run:   authorizationCreateF,
	}

	authorizationCreateCmd.Flags().StringVarP(&authorizationCreateFlags.user, "user", "u", "", "user name (required)")
	authorizationCreateCmd.MarkFlagRequired("user")

	authorizationCreateCmd.Flags().BoolVarP(&authorizationCreateFlags.createUserPermission, "create-user", "", false, "grants the permission to create users")
	authorizationCreateCmd.Flags().BoolVarP(&authorizationCreateFlags.deleteUserPermission, "delete-user", "", false, "grants the permission to delete users")

	authorizationCreateCmd.Flags().StringArrayVarP(&authorizationCreateFlags.readBucketPermissions, "read-bucket", "", []string{}, "bucket id")
	authorizationCreateCmd.Flags().StringArrayVarP(&authorizationCreateFlags.writeBucketPermissions, "write-bucket", "", []string{}, "bucket id")

	authorizationCmd.AddCommand(authorizationCreateCmd)
}

func authorizationCreateF(cmd *cobra.Command, args []string) {
	var permissions []platform.Permission
	if authorizationCreateFlags.createUserPermission {
		permissions = append(permissions, platform.CreateUserPermission)
	}
	if authorizationCreateFlags.deleteUserPermission {
		permissions = append(permissions, platform.DeleteUserPermission)
	}

	for _, p := range authorizationCreateFlags.writeBucketPermissions {
		var id platform.ID
		if err := id.DecodeFromString(p); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		permissions = append(permissions, platform.WriteBucketPermission(id))
	}
	for _, p := range authorizationCreateFlags.readBucketPermissions {
		var id platform.ID
		if err := id.DecodeFromString(p); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		permissions = append(permissions, platform.ReadBucketPermission(id))
	}

	authorization := &platform.Authorization{
		User:        authorizationCreateFlags.user,
		Permissions: permissions,
	}

	s := &http.AuthorizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	err := s.CreateAuthorization(context.Background(), authorization)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Token",
		"Status",
		"User",
		"UserID",
		"Permissions",
	)

	ps := []string{}
	for _, p := range authorization.Permissions {
		ps = append(ps, p.String())
	}

	w.Write(map[string]interface{}{
		"ID":          authorization.ID.String(),
		"Token":       authorization.Token,
		"Status":      authorization.Status,
		"User":        authorization.User,
		"UserID":      authorization.UserID.String(),
		"Permissions": ps,
	})
	w.Flush()
}

// AuthorizationFindFlags are command line args used when finding a authorization
type AuthorizationFindFlags struct {
	user   string
	userID string
	id     string
}

var authorizationFindFlags AuthorizationFindFlags

func init() {
	authorizationFindCmd := &cobra.Command{
		Use:   "find",
		Short: "Find authorization",
		Run:   authorizationFindF,
	}

	authorizationFindCmd.Flags().StringVarP(&authorizationFindFlags.user, "user", "u", "", "user")
	authorizationFindCmd.Flags().StringVarP(&authorizationFindFlags.userID, "user-id", "", "", "user ID")
	authorizationFindCmd.Flags().StringVarP(&authorizationFindFlags.id, "id", "i", "", "authorization ID")

	authorizationCmd.AddCommand(authorizationFindCmd)
}

func authorizationFindF(cmd *cobra.Command, args []string) {
	s := &http.AuthorizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	filter := platform.AuthorizationFilter{}
	if authorizationFindFlags.id != "" {
		filter.ID = &platform.ID{}
		err := filter.ID.DecodeFromString(authorizationFindFlags.id)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
	if authorizationFindFlags.user != "" {
		filter.User = &authorizationFindFlags.user
	}
	if authorizationFindFlags.userID != "" {
		filter.UserID = &platform.ID{}
		err := filter.UserID.DecodeFromString(authorizationFindFlags.userID)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	authorizations, _, err := s.FindAuthorizations(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Token",
		"Status",
		"User",
		"UserID",
		"Permissions",
	)

	for _, a := range authorizations {
		var permissions []string
		for _, p := range a.Permissions {
			permissions = append(permissions, p.String())
		}

		w.Write(map[string]interface{}{
			"ID":          a.ID,
			"Token":       a.Token,
			"Status":      a.Status,
			"User":        a.User,
			"UserID":      a.UserID.String(),
			"Permissions": permissions,
		})
	}
	w.Flush()
}

// AuthorizationDeleteFlags are command line args used when deleting a authorization
type AuthorizationDeleteFlags struct {
	id string
}

var authorizationDeleteFlags AuthorizationDeleteFlags

func init() {
	authorizationDeleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete authorization",
		Run:   authorizationDeleteF,
	}

	authorizationDeleteCmd.Flags().StringVarP(&authorizationDeleteFlags.id, "id", "i", "", "authorization id (required)")
	authorizationDeleteCmd.MarkFlagRequired("id")

	authorizationCmd.AddCommand(authorizationDeleteCmd)
}

func authorizationDeleteF(cmd *cobra.Command, args []string) {
	s := &http.AuthorizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	id := platform.ID{}
	if err := id.DecodeFromString(authorizationDeleteFlags.id); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ctx := context.TODO()
	a, err := s.FindAuthorizationByID(ctx, id)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := s.DeleteAuthorization(context.Background(), id); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Token",
		"User",
		"UserID",
		"Permissions",
		"Deleted",
	)

	ps := []string{}
	for _, p := range a.Permissions {
		ps = append(ps, p.String())
	}

	w.Write(map[string]interface{}{
		"ID":          a.ID.String(),
		"Token":       a.Token,
		"User":        a.User,
		"UserID":      a.UserID.String(),
		"Permissions": ps,
		"Deleted":     true,
	})
	w.Flush()
}

// AuthorizationActiveFlags are command line args used when enabling an authorization
type AuthorizationActiveFlags struct {
	id string
}

var authorizationActiveFlags AuthorizationActiveFlags

func init() {
	authorizationActiveCmd := &cobra.Command{
		Use:   "active",
		Short: "active authorization",
		Run:   authorizationActiveF,
	}

	authorizationActiveCmd.Flags().StringVarP(&authorizationActiveFlags.id, "id", "i", "", "authorization id (required)")
	authorizationActiveCmd.MarkFlagRequired("id")

	authorizationCmd.AddCommand(authorizationActiveCmd)
}

func authorizationActiveF(cmd *cobra.Command, args []string) {
	s := &http.AuthorizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	id := platform.ID{}
	if err := id.DecodeFromString(authorizationActiveFlags.id); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ctx := context.TODO()
	a, err := s.FindAuthorizationByID(ctx, id)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := s.SetAuthorizationStatus(context.Background(), id, platform.Active); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Token",
		"Status",
		"User",
		"UserID",
		"Permissions",
	)

	ps := []string{}
	for _, p := range a.Permissions {
		ps = append(ps, p.String())
	}

	w.Write(map[string]interface{}{
		"ID":          a.ID.String(),
		"Token":       a.Token,
		"Status":      a.Status,
		"User":        a.User,
		"UserID":      a.UserID.String(),
		"Permissions": ps,
	})
	w.Flush()
}

// AuthorizationInactiveFlags are command line args used when disabling an authorization
type AuthorizationInactiveFlags struct {
	id string
}

var authorizationInactiveFlags AuthorizationInactiveFlags

func init() {
	authorizationInactiveCmd := &cobra.Command{
		Use:   "inactive",
		Short: "inactive authorization",
		Run:   authorizationInactiveF,
	}

	authorizationInactiveCmd.Flags().StringVarP(&authorizationInactiveFlags.id, "id", "i", "", "authorization id (required)")
	authorizationInactiveCmd.MarkFlagRequired("id")

	authorizationCmd.AddCommand(authorizationInactiveCmd)
}

func authorizationInactiveF(cmd *cobra.Command, args []string) {
	s := &http.AuthorizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	id := platform.ID{}
	if err := id.DecodeFromString(authorizationInactiveFlags.id); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ctx := context.TODO()
	a, err := s.FindAuthorizationByID(ctx, id)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := s.SetAuthorizationStatus(context.Background(), id, platform.Inactive); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Token",
		"Status",
		"User",
		"UserID",
		"Permissions",
	)

	ps := []string{}
	for _, p := range a.Permissions {
		ps = append(ps, p.String())
	}

	w.Write(map[string]interface{}{
		"ID":          a.ID.String(),
		"Token":       a.Token,
		"Status":      a.Status,
		"User":        a.User,
		"UserID":      a.UserID.String(),
		"Permissions": ps,
	})
	w.Flush()
}
