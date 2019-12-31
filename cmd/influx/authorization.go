package main

import (
	"context"
	"os"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influx/internal"
	"github.com/influxdata/influxdb/http"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// AuthorizationCreateFlags are command line args used when creating a authorization
type AuthorizationCreateFlags struct {
	user string
	org  string

	writeUserPermission bool
	readUserPermission  bool

	writeBucketsPermission bool
	readBucketsPermission  bool

	writeBucketPermissions []string
	readBucketPermissions  []string

	writeTasksPermission bool
	readTasksPermission  bool

	writeTelegrafsPermission bool
	readTelegrafsPermission  bool

	writeOrganizationsPermission bool
	readOrganizationsPermission  bool

	writeDashboardsPermission bool
	readDashboardsPermission  bool

	writeCheckPermission bool
	readCheckPermission  bool

	writeNotificationRulePermission bool
	readNotificationRulePermission  bool

	writeNotificationEndpointPermission bool
	readNotificationEndpointPermission  bool
}

var authCreateFlags AuthorizationCreateFlags

func authCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "auth",
		Aliases: []string{"authorization"},
		Short:   "Authorization management commands",
		Run:     seeHelp,
	}
	cmd.AddCommand(
		authActiveCmd(),
		authCreateCmd(),
		authDeleteCmd(),
		authFindCmd(),
		authInactiveCmd(),
	)

	return cmd
}

func authCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create authorization",
		RunE:  wrapCheckSetup(authorizationCreateF),
	}

	cmd.Flags().StringVarP(&authCreateFlags.org, "org", "o", "", "The organization name (required)")
	cmd.MarkFlagRequired("org")
	viper.BindEnv("ORG")
	if h := viper.GetString("ORG"); h != "" {
		authCreateFlags.org = h
	}

	cmd.Flags().StringVarP(&authCreateFlags.user, "user", "u", "", "The user name")

	cmd.Flags().BoolVarP(&authCreateFlags.writeUserPermission, "write-user", "", false, "Grants the permission to perform mutative actions against organization users")
	cmd.Flags().BoolVarP(&authCreateFlags.readUserPermission, "read-user", "", false, "Grants the permission to perform read actions against organization users")

	cmd.Flags().BoolVarP(&authCreateFlags.writeBucketsPermission, "write-buckets", "", false, "Grants the permission to perform mutative actions against organization buckets")
	cmd.Flags().BoolVarP(&authCreateFlags.readBucketsPermission, "read-buckets", "", false, "Grants the permission to perform read actions against organization buckets")

	cmd.Flags().StringArrayVarP(&authCreateFlags.writeBucketPermissions, "write-bucket", "", []string{}, "The bucket id")
	cmd.Flags().StringArrayVarP(&authCreateFlags.readBucketPermissions, "read-bucket", "", []string{}, "The bucket id")

	cmd.Flags().BoolVarP(&authCreateFlags.writeTasksPermission, "write-tasks", "", false, "Grants the permission to create tasks")
	cmd.Flags().BoolVarP(&authCreateFlags.readTasksPermission, "read-tasks", "", false, "Grants the permission to read tasks")

	cmd.Flags().BoolVarP(&authCreateFlags.writeTelegrafsPermission, "write-telegrafs", "", false, "Grants the permission to create telegraf configs")
	cmd.Flags().BoolVarP(&authCreateFlags.readTelegrafsPermission, "read-telegrafs", "", false, "Grants the permission to read telegraf configs")

	cmd.Flags().BoolVarP(&authCreateFlags.writeOrganizationsPermission, "write-orgs", "", false, "Grants the permission to create organizations")
	cmd.Flags().BoolVarP(&authCreateFlags.readOrganizationsPermission, "read-orgs", "", false, "Grants the permission to read organizations")

	cmd.Flags().BoolVarP(&authCreateFlags.writeDashboardsPermission, "write-dashboards", "", false, "Grants the permission to create dashboards")
	cmd.Flags().BoolVarP(&authCreateFlags.readDashboardsPermission, "read-dashboards", "", false, "Grants the permission to read dashboards")

	cmd.Flags().BoolVarP(&authCreateFlags.writeNotificationRulePermission, "write-notificationRules", "", false, "Grants the permission to create notificationRules")
	cmd.Flags().BoolVarP(&authCreateFlags.readNotificationRulePermission, "read-notificationRules", "", false, "Grants the permission to read notificationRules")

	cmd.Flags().BoolVarP(&authCreateFlags.writeNotificationEndpointPermission, "write-notificationEndpoints", "", false, "Grants the permission to create notificationEndpoints")
	cmd.Flags().BoolVarP(&authCreateFlags.readNotificationEndpointPermission, "read-notificationEndpoints", "", false, "Grants the permission to read notificationEndpoints")

	cmd.Flags().BoolVarP(&authCreateFlags.writeCheckPermission, "write-checks", "", false, "Grants the permission to create checks")
	cmd.Flags().BoolVarP(&authCreateFlags.readCheckPermission, "read-checks", "", false, "Grants the permission to read checks")

	return cmd
}

func authorizationCreateF(cmd *cobra.Command, args []string) error {
	var permissions []platform.Permission
	orgSvc, err := newOrganizationService()
	if err != nil {
		return err
	}

	ctx := context.Background()
	orgFilter := platform.OrganizationFilter{Name: &authCreateFlags.org}
	o, err := orgSvc.FindOrganization(ctx, orgFilter)
	if err != nil {
		return err
	}

	bucketPerms := []struct {
		action platform.Action
		perms  []string
	}{
		{action: platform.ReadAction, perms: authCreateFlags.readBucketPermissions},
		{action: platform.WriteAction, perms: authCreateFlags.writeBucketPermissions},
	}

	for _, bp := range bucketPerms {
		for _, p := range bp.perms {
			var id platform.ID
			if err := id.DecodeFromString(p); err != nil {
				return err
			}

			p, err := platform.NewPermissionAtID(id, bp.action, platform.BucketsResourceType, o.ID)
			if err != nil {
				return err
			}

			permissions = append(permissions, *p)
		}
	}

	providedPerm := []struct {
		readPerm, writePerm bool
		ResourceType        platform.ResourceType
	}{
		{
			readPerm:     authCreateFlags.readBucketsPermission,
			writePerm:    authCreateFlags.writeBucketsPermission,
			ResourceType: platform.BucketsResourceType,
		},
		{
			readPerm:     authCreateFlags.readCheckPermission,
			writePerm:    authCreateFlags.writeCheckPermission,
			ResourceType: platform.ChecksResourceType,
		},
		{
			readPerm:     authCreateFlags.readDashboardsPermission,
			writePerm:    authCreateFlags.writeDashboardsPermission,
			ResourceType: platform.DashboardsResourceType,
		},
		{
			readPerm:     authCreateFlags.readNotificationEndpointPermission,
			writePerm:    authCreateFlags.writeNotificationEndpointPermission,
			ResourceType: platform.NotificationEndpointResourceType,
		},
		{
			readPerm:     authCreateFlags.readNotificationRulePermission,
			writePerm:    authCreateFlags.writeNotificationRulePermission,
			ResourceType: platform.NotificationRuleResourceType,
		},
		{
			readPerm:     authCreateFlags.readOrganizationsPermission,
			writePerm:    authCreateFlags.writeOrganizationsPermission,
			ResourceType: platform.OrgsResourceType,
		},
		{
			readPerm:     authCreateFlags.readTasksPermission,
			writePerm:    authCreateFlags.writeTasksPermission,
			ResourceType: platform.TasksResourceType,
		},
		{
			readPerm:     authCreateFlags.readTelegrafsPermission,
			writePerm:    authCreateFlags.writeTelegrafsPermission,
			ResourceType: platform.TelegrafsResourceType,
		},

		{
			readPerm:     authCreateFlags.readUserPermission,
			writePerm:    authCreateFlags.writeUserPermission,
			ResourceType: platform.UsersResourceType,
		},
	}

	for _, provided := range providedPerm {
		var actions []platform.Action
		if provided.readPerm {
			actions = append(actions, platform.ReadAction)
		}
		if provided.writePerm {
			actions = append(actions, platform.WriteAction)
		}

		for _, action := range actions {
			p, err := platform.NewPermission(action, provided.ResourceType, o.ID)
			if err != nil {
				return err
			}
			permissions = append(permissions, *p)
		}
	}

	authorization := &platform.Authorization{
		Permissions: permissions,
		OrgID:       o.ID,
	}

	if userName := authCreateFlags.user; userName != "" {
		userSvc, err := newUserService()
		if err != nil {
			return err
		}
		user, err := userSvc.FindUser(ctx, platform.UserFilter{
			Name: &userName,
		})
		if err != nil {
			return err
		}
		authorization.UserID = user.ID
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
	org    string
	orgID  string
	id     string
}

var authorizationFindFlags AuthorizationFindFlags

func authFindCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "find",
		Short: "Find authorization",
		RunE:  wrapCheckSetup(authorizationFindF),
	}

	cmd.Flags().StringVarP(&authorizationFindFlags.user, "user", "u", "", "The user")
	cmd.Flags().StringVarP(&authorizationFindFlags.userID, "user-id", "", "", "The user ID")
	cmd.Flags().StringVarP(&authorizationFindFlags.org, "org", "o", "", "The org")
	viper.BindEnv("ORG")
	if h := viper.GetString("ORG"); h != "" {
		authorizationFindFlags.org = h
	}

	cmd.Flags().StringVarP(&authorizationFindFlags.orgID, "org-id", "", "", "The org ID")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		authorizationFindFlags.orgID = h
	}
	cmd.Flags().StringVarP(&authorizationFindFlags.id, "id", "i", "", "The authorization ID")

	return cmd
}

func newAuthorizationService(f Flags) (platform.AuthorizationService, error) {
	if flags.local {
		return newLocalKVService()
	}

	httpClient, err := newHTTPClient()
	if err != nil {
		return nil, err
	}

	return &http.AuthorizationService{
		Client: httpClient,
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
	if authorizationFindFlags.org != "" {
		filter.Org = &authorizationFindFlags.org
	}
	if authorizationFindFlags.orgID != "" {
		oID, err := platform.IDFromString(authorizationFindFlags.orgID)
		if err != nil {
			return err
		}
		filter.OrgID = oID
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

func authDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete authorization",
		RunE:  wrapCheckSetup(authorizationDeleteF),
	}

	cmd.Flags().StringVarP(&authorizationDeleteFlags.id, "id", "i", "", "The authorization ID (required)")
	cmd.MarkFlagRequired("id")

	return cmd
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

func authActiveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "active",
		Short: "Active authorization",
		RunE:  wrapCheckSetup(authorizationActiveF),
	}

	cmd.Flags().StringVarP(&authorizationActiveFlags.id, "id", "i", "", "The authorization ID (required)")
	cmd.MarkFlagRequired("id")

	return cmd
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
	if _, err := s.FindAuthorizationByID(ctx, id); err != nil {
		return err
	}

	a, err := s.UpdateAuthorization(context.Background(), id, &platform.AuthorizationUpdate{
		Status: platform.Active.Ptr(),
	})
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

func authInactiveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inactive",
		Short: "Inactive authorization",
		RunE:  wrapCheckSetup(authorizationInactiveF),
	}

	cmd.Flags().StringVarP(&authorizationInactiveFlags.id, "id", "i", "", "The authorization ID (required)")
	cmd.MarkFlagRequired("id")

	return cmd
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
	if _, err = s.FindAuthorizationByID(ctx, id); err != nil {
		return err
	}

	a, err := s.UpdateAuthorization(context.Background(), id, &platform.AuthorizationUpdate{
		Status: platform.Inactive.Ptr(),
	})
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
