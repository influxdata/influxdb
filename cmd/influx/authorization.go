package main

import (
	"context"
	"os"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influx/internal"
	"github.com/influxdata/influxdb/http"
	"github.com/spf13/cobra"
)

func cmdAuth(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := opt.newCmd("auth", nil)
	cmd.Aliases = []string{"authorization"}
	cmd.Short = "Authorization management commands"
	cmd.Run = seeHelp

	cmd.AddCommand(
		authActiveCmd(),
		authCreateCmd(),
		authDeleteCmd(),
		authFindCmd(),
		authInactiveCmd(),
	)

	return cmd
}

var authCreateFlags struct {
	user string
	org  organization

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

func authCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create authorization",
		RunE:  checkSetupRunEMiddleware(&flags)(authorizationCreateF),
	}
	authCreateFlags.org.register(cmd, false)

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
	if err := authCreateFlags.org.validOrgFlags(&flags); err != nil {
		return err
	}

	orgSvc, err := newOrganizationService()
	if err != nil {
		return err
	}

	orgID, err := authCreateFlags.org.getID(orgSvc)
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

	var permissions []platform.Permission
	for _, bp := range bucketPerms {
		for _, p := range bp.perms {
			var id platform.ID
			if err := id.DecodeFromString(p); err != nil {
				return err
			}

			p, err := platform.NewPermissionAtID(id, bp.action, platform.BucketsResourceType, orgID)
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
			p, err := platform.NewPermission(action, provided.ResourceType, orgID)
			if err != nil {
				return err
			}
			permissions = append(permissions, *p)
		}
	}

	authorization := &platform.Authorization{
		Permissions: permissions,
		OrgID:       orgID,
	}

	if userName := authCreateFlags.user; userName != "" {
		userSvc, err := newUserService()
		if err != nil {
			return err
		}
		user, err := userSvc.FindUser(context.Background(), platform.UserFilter{
			Name: &userName,
		})
		if err != nil {
			return err
		}
		authorization.UserID = user.ID
	}

	s, err := newAuthorizationService()
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

var authorizationFindFlags struct {
	org    organization
	user   string
	userID string
	id     string
}

func authFindCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Short:   "List authorizations",
		Aliases: []string{"find", "ls"},
		RunE:    checkSetupRunEMiddleware(&flags)(authorizationFindF),
	}

	cmd.Flags().StringVarP(&authorizationFindFlags.user, "user", "u", "", "The user")
	cmd.Flags().StringVarP(&authorizationFindFlags.userID, "user-id", "", "", "The user ID")

	cmd.Flags().StringVarP(&authorizationFindFlags.id, "id", "i", "", "The authorization ID")

	return cmd
}

func newAuthorizationService() (platform.AuthorizationService, error) {
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
	s, err := newAuthorizationService()
	if err != nil {
		return err
	}

	us, err := newUserService()
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
	if authorizationFindFlags.org.name != "" {
		filter.Org = &authorizationFindFlags.org.name
	}
	if authorizationFindFlags.org.id != "" {
		oID, err := platform.IDFromString(authorizationFindFlags.org.id)
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
		user, err := us.FindUserByID(context.Background(), a.UserID)
		if err != nil {
			return err
		}

		w.Write(map[string]interface{}{
			"ID":          a.ID,
			"Token":       a.Token,
			"Status":      a.Status,
			"User":        user.Name,
			"UserID":      a.UserID.String(),
			"Permissions": permissions,
		})
	}

	w.Flush()

	return nil
}

var authorizationDeleteFlags struct {
	id string
}

func authDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete authorization",
		RunE:  checkSetupRunEMiddleware(&flags)(authorizationDeleteF),
	}

	cmd.Flags().StringVarP(&authorizationDeleteFlags.id, "id", "i", "", "The authorization ID (required)")
	cmd.MarkFlagRequired("id")

	return cmd
}

func authorizationDeleteF(cmd *cobra.Command, args []string) error {
	s, err := newAuthorizationService()
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

var authorizationActiveFlags struct {
	id string
}

func authActiveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "active",
		Short: "Active authorization",
		RunE:  checkSetupRunEMiddleware(&flags)(authorizationActiveF),
	}

	cmd.Flags().StringVarP(&authorizationActiveFlags.id, "id", "i", "", "The authorization ID (required)")
	cmd.MarkFlagRequired("id")

	return cmd
}

func authorizationActiveF(cmd *cobra.Command, args []string) error {
	s, err := newAuthorizationService()
	if err != nil {
		return err
	}

	us, err := newUserService()
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

	user, err := us.FindUserByID(context.Background(), a.UserID)
	if err != nil {
		return err
	}

	w.Write(map[string]interface{}{
		"ID":          a.ID.String(),
		"Token":       a.Token,
		"Status":      a.Status,
		"User":        user.Name,
		"UserID":      a.UserID.String(),
		"Permissions": ps,
	})

	w.Flush()

	return nil
}

var authorizationInactiveFlags struct {
	id string
}

func authInactiveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inactive",
		Short: "Inactive authorization",
		RunE:  checkSetupRunEMiddleware(&flags)(authorizationInactiveF),
	}

	cmd.Flags().StringVarP(&authorizationInactiveFlags.id, "id", "i", "", "The authorization ID (required)")
	cmd.MarkFlagRequired("id")

	return cmd
}

func authorizationInactiveF(cmd *cobra.Command, args []string) error {
	s, err := newAuthorizationService()
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
