package main

import (
	"context"
	"os"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/cmd/influx/internal"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/internal/fs"
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
		RunE:  authorizationCreateF,
	}

	authorizationCreateCmd.Flags().StringVarP(&authorizationCreateFlags.user, "user", "u", "", "user name (required)")
	authorizationCreateCmd.MarkFlagRequired("user")

	authorizationCreateCmd.Flags().BoolVarP(&authorizationCreateFlags.createUserPermission, "create-user", "", false, "grants the permission to create users")
	authorizationCreateCmd.Flags().BoolVarP(&authorizationCreateFlags.deleteUserPermission, "delete-user", "", false, "grants the permission to delete users")

	authorizationCreateCmd.Flags().StringArrayVarP(&authorizationCreateFlags.readBucketPermissions, "read-bucket", "", []string{}, "bucket id")
	authorizationCreateCmd.Flags().StringArrayVarP(&authorizationCreateFlags.writeBucketPermissions, "write-bucket", "", []string{}, "bucket id")

	authorizationCmd.AddCommand(authorizationCreateCmd)
}

func authorizationCreateF(cmd *cobra.Command, args []string) error {
	var permissions []platform.Permission
	if authorizationCreateFlags.createUserPermission {
		p, err := platform.NewPermission(platform.WriteAction, platform.UsersResource)
		if err != nil {
			return err
		}
		permissions = append(permissions, *p)
	}

	if authorizationCreateFlags.deleteUserPermission {
		p, err := platform.NewPermission(platform.WriteAction, platform.UsersResource)
		if err != nil {
			return err
		}
		permissions = append(permissions, *p)
	}

	for _, p := range authorizationCreateFlags.writeBucketPermissions {
		var id platform.ID
		if err := id.DecodeFromString(p); err != nil {
			return err
		}

		p, err := platform.NewPermissionAtID(id, platform.WriteAction, platform.BucketsResource)
		if err != nil {
			return err
		}

		permissions = append(permissions, *p)
	}

	for _, p := range authorizationCreateFlags.readBucketPermissions {
		var id platform.ID
		if err := id.DecodeFromString(p); err != nil {
			return err
		}

		p, err := platform.NewPermissionAtID(id, platform.ReadAction, platform.BucketsResource)
		if err != nil {
			return err
		}
		permissions = append(permissions, *p)
	}

	authorization := &platform.Authorization{
		Permissions: permissions,
	}

	s, err := newAuthorizationService(flags)
	if err != nil {
		return err
	}

	if err := s.CreateAuthorization(context.Background(), authorization); err != nil {
		return err
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
		"UserID":      authorization.UserID.String(),
		"Permissions": ps,
	})

	w.Flush()

	return nil
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
		RunE:  authorizationFindF,
	}

	authorizationFindCmd.Flags().StringVarP(&authorizationFindFlags.user, "user", "u", "", "user")
	authorizationFindCmd.Flags().StringVarP(&authorizationFindFlags.userID, "user-id", "", "", "user ID")
	authorizationFindCmd.Flags().StringVarP(&authorizationFindFlags.id, "id", "i", "", "authorization ID")

	authorizationCmd.AddCommand(authorizationFindCmd)
}

func newAuthorizationService(f Flags) (platform.AuthorizationService, error) {
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
	return &http.AuthorizationService{
		Addr:  flags.host,
		Token: flags.token,
	}, nil
}

func authorizationFindF(cmd *cobra.Command, args []string) error {
	s, err := newAuthorizationService(flags)
	if err != nil {
		return err
	}

	filter := platform.AuthorizationFilter{}
	if authorizationFindFlags.id != "" {
		fID, err := platform.IDFromString(authorizationFindFlags.id)
		if err != nil {
			return err
		}
		filter.ID = fID
	}
	if authorizationFindFlags.user != "" {
		filter.User = &authorizationFindFlags.user
	}
	if authorizationFindFlags.userID != "" {
		uID, err := platform.IDFromString(authorizationFindFlags.userID)
		if err != nil {
			return err
		}
		filter.UserID = uID
	}

	authorizations, _, err := s.FindAuthorizations(context.Background(), filter)
	if err != nil {
		return err
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
			"UserID":      a.UserID.String(),
			"Permissions": permissions,
		})
	}

	w.Flush()

	return nil
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
		RunE:  authorizationDeleteF,
	}

	authorizationDeleteCmd.Flags().StringVarP(&authorizationDeleteFlags.id, "id", "i", "", "authorization id (required)")
	authorizationDeleteCmd.MarkFlagRequired("id")

	authorizationCmd.AddCommand(authorizationDeleteCmd)
}

func authorizationDeleteF(cmd *cobra.Command, args []string) error {
	s, err := newAuthorizationService(flags)
	if err != nil {
		return err
	}

	id, err := platform.IDFromString(authorizationDeleteFlags.id)
	if err != nil {
		return err
	}

	ctx := context.TODO()
	a, err := s.FindAuthorizationByID(ctx, *id)
	if err != nil {
		return err
	}

	if err := s.DeleteAuthorization(context.Background(), *id); err != nil {
		return err
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
		"UserID":      a.UserID.String(),
		"Permissions": ps,
		"Deleted":     true,
	})

	w.Flush()

	return nil
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
		RunE:  authorizationActiveF,
	}

	authorizationActiveCmd.Flags().StringVarP(&authorizationActiveFlags.id, "id", "i", "", "authorization id (required)")
	authorizationActiveCmd.MarkFlagRequired("id")

	authorizationCmd.AddCommand(authorizationActiveCmd)
}

func authorizationActiveF(cmd *cobra.Command, args []string) error {
	s, err := newAuthorizationService(flags)
	if err != nil {
		return err
	}

	var id platform.ID
	if err := id.DecodeFromString(authorizationActiveFlags.id); err != nil {
		return err
	}

	ctx := context.TODO()
	a, err := s.FindAuthorizationByID(ctx, id)
	if err != nil {
		return err
	}

	if err := s.SetAuthorizationStatus(context.Background(), id, platform.Active); err != nil {
		return err
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
		"UserID":      a.UserID.String(),
		"Permissions": ps,
	})

	w.Flush()

	return nil
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
		RunE:  authorizationInactiveF,
	}

	authorizationInactiveCmd.Flags().StringVarP(&authorizationInactiveFlags.id, "id", "i", "", "authorization id (required)")
	authorizationInactiveCmd.MarkFlagRequired("id")

	authorizationCmd.AddCommand(authorizationInactiveCmd)
}

func authorizationInactiveF(cmd *cobra.Command, args []string) error {
	s, err := newAuthorizationService(flags)
	if err != nil {
		return err
	}

	var id platform.ID
	if err := id.DecodeFromString(authorizationInactiveFlags.id); err != nil {
		return err
	}

	ctx := context.TODO()
	a, err := s.FindAuthorizationByID(ctx, id)
	if err != nil {
		return err
	}

	if err := s.SetAuthorizationStatus(ctx, id, platform.Inactive); err != nil {
		return err
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
		"UserID":      a.UserID.String(),
		"Permissions": ps,
	})

	w.Flush()

	return nil
}
