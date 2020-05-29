package main

import (
	"context"
	"io"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influx/internal"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/spf13/cobra"
)

type token struct {
	ID          platform.ID `json:"id"`
	Token       string      `json:"token"`
	Status      string      `json:"status"`
	UserName    string      `json:"userName"`
	UserID      platform.ID `json:"userID"`
	Permissions []string    `json:"permissions"`
}

func cmdAuth(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := opt.newCmd("auth", nil, false)
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

var authCRUDFlags struct {
	id          string
	json        bool
	hideHeaders bool
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

	writeDBRPPermission bool
	readDBRPPermission  bool
}

func authCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create authorization",
		RunE:  checkSetupRunEMiddleware(&flags)(authorizationCreateF),
	}
	authCreateFlags.org.register(cmd, false)

	cmd.Flags().StringVarP(&authCreateFlags.user, "user", "u", "", "The user name")
	registerPrintOptions(cmd, &authCRUDFlags.hideHeaders, &authCRUDFlags.json)

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

	cmd.Flags().BoolVarP(&authCreateFlags.writeDBRPPermission, "write-dbrps", "", false, "Grants the permission to create database retention policy mappings")
	cmd.Flags().BoolVarP(&authCreateFlags.readDBRPPermission, "read-dbrps", "", false, "Grants the permission to read database retention policy mappings")

	return cmd
}

func authorizationCreateF(cmd *cobra.Command, args []string) error {
	if err := authCreateFlags.org.validOrgFlags(&flags); err != nil {
		return err
	}

	userSvc, err := newUserService()
	if err != nil {
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
		{
			readPerm:     authCreateFlags.readDBRPPermission,
			writePerm:    authCreateFlags.writeDBRPPermission,
			ResourceType: platform.DBRPResourceType,
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

	user, err := userSvc.FindUserByID(context.Background(), authorization.UserID)
	if err != nil {
		return err
	}

	ps := make([]string, 0, len(authorization.Permissions))
	for _, p := range authorization.Permissions {
		ps = append(ps, p.String())
	}

	return writeTokens(cmd.OutOrStdout(), tokenPrintOpt{
		jsonOut:     authCRUDFlags.json,
		hideHeaders: authCRUDFlags.hideHeaders,
		token: token{
			ID:          authorization.ID,
			Token:       authorization.Token,
			Status:      string(authorization.Status),
			UserName:    user.Name,
			UserID:      user.ID,
			Permissions: ps,
		},
	})
}

var authorizationFindFlags struct {
	org    organization
	user   string
	userID string
}

func authFindCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Short:   "List authorizations",
		Aliases: []string{"find", "ls"},
		RunE:    checkSetupRunEMiddleware(&flags)(authorizationFindF),
	}

	authorizationFindFlags.org.register(cmd, false)
	registerPrintOptions(cmd, &authCRUDFlags.hideHeaders, &authCRUDFlags.json)
	cmd.Flags().StringVarP(&authorizationFindFlags.user, "user", "u", "", "The user")
	cmd.Flags().StringVarP(&authorizationFindFlags.userID, "user-id", "", "", "The user ID")

	cmd.Flags().StringVarP(&authCRUDFlags.id, "id", "i", "", "The authorization ID")

	return cmd
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

	var filter platform.AuthorizationFilter
	if authCRUDFlags.id != "" {
		fID, err := platform.IDFromString(authCRUDFlags.id)
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

	var tokens []token
	for _, a := range authorizations {
		var permissions []string
		for _, p := range a.Permissions {
			permissions = append(permissions, p.String())
		}

		user, err := us.FindUserByID(context.Background(), a.UserID)
		if err != nil {
			return err
		}

		tokens = append(tokens, token{
			ID:          a.ID,
			Token:       a.Token,
			Status:      string(a.Status),
			UserName:    user.Name,
			UserID:      a.UserID,
			Permissions: permissions,
		})
	}

	return writeTokens(cmd.OutOrStdout(), tokenPrintOpt{
		jsonOut:     authCRUDFlags.json,
		hideHeaders: authCRUDFlags.hideHeaders,
		tokens:      tokens,
	})
}

func authDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete authorization",
		RunE:  checkSetupRunEMiddleware(&flags)(authorizationDeleteF),
	}

	registerPrintOptions(cmd, &authCRUDFlags.hideHeaders, &authCRUDFlags.json)
	cmd.Flags().StringVarP(&authCRUDFlags.id, "id", "i", "", "The authorization ID (required)")
	cmd.MarkFlagRequired("id")

	return cmd
}

func authorizationDeleteF(cmd *cobra.Command, args []string) error {
	s, err := newAuthorizationService()
	if err != nil {
		return err
	}

	us, err := newUserService()
	if err != nil {
		return err
	}

	id, err := platform.IDFromString(authCRUDFlags.id)
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

	user, err := us.FindUserByID(context.Background(), a.UserID)
	if err != nil {
		return err
	}

	ps := make([]string, 0, len(a.Permissions))
	for _, p := range a.Permissions {
		ps = append(ps, p.String())
	}

	return writeTokens(cmd.OutOrStdout(), tokenPrintOpt{
		jsonOut:     authCRUDFlags.json,
		deleted:     true,
		hideHeaders: authCRUDFlags.hideHeaders,
		token: token{
			ID:          a.ID,
			Token:       a.Token,
			Status:      string(a.Status),
			UserName:    user.Name,
			UserID:      user.ID,
			Permissions: ps,
		},
	})
}

func authActiveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "active",
		Short: "Active authorization",
		RunE:  checkSetupRunEMiddleware(&flags)(authorizationActiveF),
	}

	registerPrintOptions(cmd, &authCRUDFlags.hideHeaders, &authCRUDFlags.json)
	cmd.Flags().StringVarP(&authCRUDFlags.id, "id", "i", "", "The authorization ID (required)")
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
	if err := id.DecodeFromString(authCRUDFlags.id); err != nil {
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

	user, err := us.FindUserByID(context.Background(), a.UserID)
	if err != nil {
		return err
	}

	ps := make([]string, 0, len(a.Permissions))
	for _, p := range a.Permissions {
		ps = append(ps, p.String())
	}

	return writeTokens(cmd.OutOrStdout(), tokenPrintOpt{
		jsonOut:     authCRUDFlags.json,
		hideHeaders: authCRUDFlags.hideHeaders,
		token: token{
			ID:          a.ID,
			Token:       a.Token,
			Status:      string(a.Status),
			UserName:    user.Name,
			UserID:      user.ID,
			Permissions: ps,
		},
	})
}

func authInactiveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inactive",
		Short: "Inactive authorization",
		RunE:  checkSetupRunEMiddleware(&flags)(authorizationInactiveF),
	}

	registerPrintOptions(cmd, &authCRUDFlags.hideHeaders, &authCRUDFlags.json)
	cmd.Flags().StringVarP(&authCRUDFlags.id, "id", "i", "", "The authorization ID (required)")
	cmd.MarkFlagRequired("id")

	return cmd
}

func authorizationInactiveF(cmd *cobra.Command, args []string) error {
	s, err := newAuthorizationService()
	if err != nil {
		return err
	}

	us, err := newUserService()
	if err != nil {
		return err
	}

	var id platform.ID
	if err := id.DecodeFromString(authCRUDFlags.id); err != nil {
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

	user, err := us.FindUserByID(context.Background(), a.UserID)
	if err != nil {
		return err
	}

	ps := make([]string, 0, len(a.Permissions))
	for _, p := range a.Permissions {
		ps = append(ps, p.String())
	}

	return writeTokens(cmd.OutOrStdout(), tokenPrintOpt{
		jsonOut:     authCRUDFlags.json,
		hideHeaders: authCRUDFlags.hideHeaders,
		token: token{
			ID:          a.ID,
			Token:       a.Token,
			Status:      string(a.Status),
			UserName:    user.Name,
			UserID:      user.ID,
			Permissions: ps,
		},
	})
}

type tokenPrintOpt struct {
	jsonOut     bool
	deleted     bool
	hideHeaders bool
	token       token
	tokens      []token
}

func writeTokens(w io.Writer, printOpts tokenPrintOpt) error {
	if printOpts.jsonOut {
		var v interface{} = printOpts.tokens
		if printOpts.tokens == nil {
			v = printOpts.token
		}
		return writeJSON(w, v)
	}

	tabW := internal.NewTabWriter(w)
	defer tabW.Flush()

	tabW.HideHeaders(printOpts.hideHeaders)

	headers := []string{
		"ID",
		"Token",
		"User Name",
		"User ID",
		"Permissions",
	}
	if printOpts.deleted {
		headers = append(headers, "Deleted")
	}
	tabW.WriteHeaders(headers...)

	if printOpts.tokens == nil {
		printOpts.tokens = append(printOpts.tokens, printOpts.token)
	}

	for _, t := range printOpts.tokens {
		m := map[string]interface{}{
			"ID":          t.ID.String(),
			"Token":       t.Token,
			"User Name":   t.UserName,
			"User ID":     t.UserID.String(),
			"Permissions": t.Permissions,
		}
		if printOpts.deleted {
			m["Deleted"] = true
		}
		tabW.Write(m)
	}

	return nil
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
