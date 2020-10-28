package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influx/internal"
	cinternal "github.com/influxdata/influxdb/v2/cmd/internal"
	"github.com/influxdata/influxdb/v2/v1/authorization"
	"github.com/spf13/cobra"
	"github.com/tcnksm/go-input"
)

type v1Token struct {
	ID          influxdb.ID `json:"id"`
	Description string      `json:"description"`
	Token       string      `json:"token"`
	Status      string      `json:"status"`
	UserName    string      `json:"userName"`
	UserID      influxdb.ID `json:"userID"`
	Permissions []string    `json:"permissions"`
}

func cmdV1Auth(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := opt.newCmd("auth", nil, false)
	cmd.Aliases = []string{"authorization"}
	cmd.Short = "Authorization management commands for v1 APIs"
	cmd.Run = seeHelp

	cmd.AddCommand(
		v1AuthActiveCmd(f),
		v1AuthCreateCmd(f),
		v1AuthDeleteCmd(f),
		v1AuthFindCmd(f),
		v1AuthInactiveCmd(f),
		v1AuthSetPasswordCmd(f, opt),
	)

	return cmd
}

var v1AuthCRUDFlags struct {
	id          string
	json        bool
	hideHeaders bool
}

var v1AuthCreateFlags struct {
	token       string
	user        string
	description string
	org         organization

	writeBucketPermissions []string
	readBucketPermissions  []string
}

func v1AuthCreateCmd(f *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create authorization",
		RunE:  checkSetupRunEMiddleware(&flags)(v1AuthorizationCreateF),
	}

	f.registerFlags(cmd)
	v1AuthCreateFlags.org.register(cmd, false)

	cmd.Flags().StringVar(&v1AuthCreateFlags.token, "auth-token", "", "Token, formatted as username:password")
	cmd.MarkFlagRequired("auth-token")
	cmd.Flags().StringVarP(&v1AuthCreateFlags.description, "description", "d", "", "Token description")
	cmd.Flags().StringVarP(&v1AuthCreateFlags.user, "user", "u", "", "The user name")
	registerPrintOptions(cmd, &v1AuthCRUDFlags.hideHeaders, &v1AuthCRUDFlags.json)

	cmd.Flags().StringArrayVarP(&v1AuthCreateFlags.writeBucketPermissions, "write-bucket", "", []string{}, "The bucket id")
	cmd.Flags().StringArrayVarP(&v1AuthCreateFlags.readBucketPermissions, "read-bucket", "", []string{}, "The bucket id")

	return cmd
}

func v1AuthorizationCreateF(cmd *cobra.Command, args []string) error {
	if err := v1AuthCreateFlags.org.validOrgFlags(&flags); err != nil {
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

	orgID, err := v1AuthCreateFlags.org.getID(orgSvc)
	if err != nil {
		return err
	}

	bucketPerms := []struct {
		action influxdb.Action
		perms  []string
	}{
		{action: influxdb.ReadAction, perms: v1AuthCreateFlags.readBucketPermissions},
		{action: influxdb.WriteAction, perms: v1AuthCreateFlags.writeBucketPermissions},
	}

	var permissions []influxdb.Permission
	for _, bp := range bucketPerms {
		for _, p := range bp.perms {
			var id influxdb.ID
			if err := id.DecodeFromString(p); err != nil {
				return err
			}

			p, err := influxdb.NewPermissionAtID(id, bp.action, influxdb.BucketsResourceType, orgID)
			if err != nil {
				return err
			}

			permissions = append(permissions, *p)
		}
	}

	auth := &influxdb.Authorization{
		Token:       v1AuthCreateFlags.token,
		Description: v1AuthCreateFlags.description,
		Permissions: permissions,
		OrgID:       orgID,
	}

	if userName := v1AuthCreateFlags.user; userName != "" {
		user, err := userSvc.FindUser(context.Background(), influxdb.UserFilter{
			Name: &userName,
		})
		if err != nil {
			return err
		}
		auth.UserID = user.ID
	}

	s, err := newV1AuthorizationService()
	if err != nil {
		return err
	}

	if err := s.CreateAuthorization(context.Background(), auth); err != nil {
		return err
	}

	user, err := userSvc.FindUserByID(context.Background(), auth.UserID)
	if err != nil {
		return err
	}

	ps := make([]string, 0, len(auth.Permissions))
	for _, p := range auth.Permissions {
		ps = append(ps, p.String())
	}

	return v1WriteTokens(cmd.OutOrStdout(), v1TokenPrintOpt{
		jsonOut:     v1AuthCRUDFlags.json,
		hideHeaders: v1AuthCRUDFlags.hideHeaders,
		token: v1Token{
			ID:          auth.ID,
			Description: auth.Description,
			Token:       auth.Token,
			Status:      string(auth.Status),
			UserName:    user.Name,
			UserID:      user.ID,
			Permissions: ps,
		},
	})
}

var v1AuthorizationFindFlags struct {
	org    organization
	user   string
	userID string
}

func v1AuthFindCmd(f *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Short:   "List authorizations",
		Aliases: []string{"find", "ls"},
		RunE:    checkSetupRunEMiddleware(&flags)(v1AuthorizationFindF),
	}

	f.registerFlags(cmd)
	v1AuthorizationFindFlags.org.register(cmd, false)
	registerPrintOptions(cmd, &v1AuthCRUDFlags.hideHeaders, &v1AuthCRUDFlags.json)
	cmd.Flags().StringVarP(&v1AuthorizationFindFlags.user, "user", "u", "", "The user")
	cmd.Flags().StringVarP(&v1AuthorizationFindFlags.userID, "user-id", "", "", "The user ID")

	cmd.Flags().StringVarP(&v1AuthCRUDFlags.id, "id", "i", "", "The authorization ID")

	return cmd
}

func v1AuthorizationFindF(cmd *cobra.Command, args []string) error {
	s, err := newV1AuthorizationService()
	if err != nil {
		return err
	}

	us, err := newUserService()
	if err != nil {
		return err
	}

	var filter influxdb.AuthorizationFilter
	if v1AuthCRUDFlags.id != "" {
		fID, err := influxdb.IDFromString(v1AuthCRUDFlags.id)
		if err != nil {
			return err
		}
		filter.ID = fID
	}
	if v1AuthorizationFindFlags.user != "" {
		filter.User = &v1AuthorizationFindFlags.user
	}
	if v1AuthorizationFindFlags.userID != "" {
		uID, err := influxdb.IDFromString(v1AuthorizationFindFlags.userID)
		if err != nil {
			return err
		}
		filter.UserID = uID
	}
	if v1AuthorizationFindFlags.org.name != "" {
		filter.Org = &v1AuthorizationFindFlags.org.name
	}
	if v1AuthorizationFindFlags.org.id != "" {
		oID, err := influxdb.IDFromString(v1AuthorizationFindFlags.org.id)
		if err != nil {
			return err
		}
		filter.OrgID = oID
	}

	authorizations, _, err := s.FindAuthorizations(context.Background(), filter)
	if err != nil {
		return err
	}

	var tokens []v1Token
	for _, a := range authorizations {
		var permissions []string
		for _, p := range a.Permissions {
			permissions = append(permissions, p.String())
		}

		user, err := us.FindUserByID(context.Background(), a.UserID)
		if err != nil {
			return err
		}

		tokens = append(tokens, v1Token{
			ID:          a.ID,
			Description: a.Description,
			Token:       a.Token,
			Status:      string(a.Status),
			UserName:    user.Name,
			UserID:      a.UserID,
			Permissions: permissions,
		})
	}

	return v1WriteTokens(cmd.OutOrStdout(), v1TokenPrintOpt{
		jsonOut:     v1AuthCRUDFlags.json,
		hideHeaders: v1AuthCRUDFlags.hideHeaders,
		tokens:      tokens,
	})
}

func v1AuthDeleteCmd(f *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete authorization",
		RunE:  checkSetupRunEMiddleware(&flags)(v1AuthorizationDeleteF),
	}

	f.registerFlags(cmd)
	registerPrintOptions(cmd, &v1AuthCRUDFlags.hideHeaders, &v1AuthCRUDFlags.json)
	cmd.Flags().StringVarP(&v1AuthCRUDFlags.id, "id", "i", "", "The authorization ID (required)")
	cmd.MarkFlagRequired("id")

	return cmd
}

func v1AuthorizationDeleteF(cmd *cobra.Command, args []string) error {
	s, err := newV1AuthorizationService()
	if err != nil {
		return err
	}

	us, err := newUserService()
	if err != nil {
		return err
	}

	id, err := influxdb.IDFromString(v1AuthCRUDFlags.id)
	if err != nil {
		return err
	}

	ctx := context.Background()
	a, err := s.FindAuthorizationByID(ctx, *id)
	if err != nil {
		return err
	}

	if err := s.DeleteAuthorization(ctx, *id); err != nil {
		return err
	}

	user, err := us.FindUserByID(ctx, a.UserID)
	if err != nil {
		return err
	}

	ps := make([]string, 0, len(a.Permissions))
	for _, p := range a.Permissions {
		ps = append(ps, p.String())
	}

	return v1WriteTokens(cmd.OutOrStdout(), v1TokenPrintOpt{
		jsonOut:     v1AuthCRUDFlags.json,
		deleted:     true,
		hideHeaders: v1AuthCRUDFlags.hideHeaders,
		token: v1Token{
			ID:          a.ID,
			Description: a.Description,
			Token:       a.Token,
			Status:      string(a.Status),
			UserName:    user.Name,
			UserID:      user.ID,
			Permissions: ps,
		},
	})
}

func v1AuthActiveCmd(f *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "active",
		Short: "Active authorization",
		RunE:  checkSetupRunEMiddleware(&flags)(v1AuthorizationActiveF),
	}
	f.registerFlags(cmd)

	registerPrintOptions(cmd, &v1AuthCRUDFlags.hideHeaders, &v1AuthCRUDFlags.json)
	cmd.Flags().StringVarP(&v1AuthCRUDFlags.id, "id", "i", "", "The authorization ID (required)")
	cmd.MarkFlagRequired("id")

	return cmd
}

func v1AuthorizationActiveF(cmd *cobra.Command, args []string) error {
	s, err := newV1AuthorizationService()
	if err != nil {
		return err
	}

	us, err := newUserService()
	if err != nil {
		return err
	}

	var id influxdb.ID
	if err := id.DecodeFromString(v1AuthCRUDFlags.id); err != nil {
		return err
	}

	ctx := context.Background()
	if _, err := s.FindAuthorizationByID(ctx, id); err != nil {
		return err
	}

	a, err := s.UpdateAuthorization(ctx, id, &influxdb.AuthorizationUpdate{
		Status: influxdb.Active.Ptr(),
	})
	if err != nil {
		return err
	}

	user, err := us.FindUserByID(ctx, a.UserID)
	if err != nil {
		return err
	}

	ps := make([]string, 0, len(a.Permissions))
	for _, p := range a.Permissions {
		ps = append(ps, p.String())
	}

	return v1WriteTokens(cmd.OutOrStdout(), v1TokenPrintOpt{
		jsonOut:     v1AuthCRUDFlags.json,
		hideHeaders: v1AuthCRUDFlags.hideHeaders,
		token: v1Token{
			ID:          a.ID,
			Description: a.Description,
			Token:       a.Token,
			Status:      string(a.Status),
			UserName:    user.Name,
			UserID:      user.ID,
			Permissions: ps,
		},
	})
}

func v1AuthInactiveCmd(f *globalFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inactive",
		Short: "Inactive authorization",
		RunE:  checkSetupRunEMiddleware(&flags)(v1AuthorizationInactiveF),
	}

	f.registerFlags(cmd)
	registerPrintOptions(cmd, &v1AuthCRUDFlags.hideHeaders, &v1AuthCRUDFlags.json)
	cmd.Flags().StringVarP(&v1AuthCRUDFlags.id, "id", "i", "", "The authorization ID (required)")
	cmd.MarkFlagRequired("id")

	return cmd
}

func v1AuthorizationInactiveF(cmd *cobra.Command, args []string) error {
	s, err := newV1AuthorizationService()
	if err != nil {
		return err
	}

	us, err := newUserService()
	if err != nil {
		return err
	}

	var id influxdb.ID
	if err := id.DecodeFromString(v1AuthCRUDFlags.id); err != nil {
		return err
	}

	ctx := context.Background()
	if _, err = s.FindAuthorizationByID(ctx, id); err != nil {
		return err
	}

	a, err := s.UpdateAuthorization(ctx, id, &influxdb.AuthorizationUpdate{
		Status: influxdb.Inactive.Ptr(),
	})
	if err != nil {
		return err
	}

	user, err := us.FindUserByID(ctx, a.UserID)
	if err != nil {
		return err
	}

	ps := make([]string, 0, len(a.Permissions))
	for _, p := range a.Permissions {
		ps = append(ps, p.String())
	}

	return v1WriteTokens(cmd.OutOrStdout(), v1TokenPrintOpt{
		jsonOut:     v1AuthCRUDFlags.json,
		hideHeaders: v1AuthCRUDFlags.hideHeaders,
		token: v1Token{
			ID:          a.ID,
			Description: a.Description,
			Token:       a.Token,
			Status:      string(a.Status),
			UserName:    user.Name,
			UserID:      user.ID,
			Permissions: ps,
		},
	})
}

var v1AuthSetPasswordFlags struct {
	id   string
	name string
}

func v1AuthSetPasswordCmd(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-password",
		Short: "Set a password for an existing authorization",
		RunE:  checkSetupRunEMiddleware(&flags)(makeV1AuthorizationSetPasswordF(opt)),
	}

	f.registerFlags(cmd)
	cmd.Flags().StringVarP(&v1AuthSetPasswordFlags.id, "id", "i", "", "The authorization ID")
	cmd.Flags().StringVarP(&v1AuthSetPasswordFlags.name, "name", "n", "", "The authorization token (username)")

	return cmd
}

func makeV1AuthorizationSetPasswordF(opt genericCLIOpts) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		filter := influxdb.AuthorizationFilter{}
		if v1AuthSetPasswordFlags.name != "" {
			filter.Token = &v1AuthSetPasswordFlags.name
		}
		if v1AuthSetPasswordFlags.id != "" {
			id, err := influxdb.IDFromString(v1AuthSetPasswordFlags.id)
			if err != nil {
				return err
			}
			filter.ID = id
		}

		s, err := newV1AuthorizationService()
		if err != nil {
			return err
		}

		authorizations, _, err := s.FindAuthorizations(context.Background(), filter)
		if err != nil {
			return err
		}

		if len(authorizations) > 1 {
			return errors.New("multiple authorizations found")
		}

		if len(authorizations) == 0 {
			return errors.New("no authorizations found")
		}

		ui := &input.UI{
			Writer: opt.w,
			Reader: opt.in,
		}

		password := cinternal.GetPassword(ui, false)

		if err := s.SetPassword(context.Background(), authorizations[0].ID, password); err != nil {
			return fmt.Errorf("error setting password: %w", err)
		}

		return nil
	}
}

type v1TokenPrintOpt struct {
	jsonOut     bool
	deleted     bool
	hideHeaders bool
	token       v1Token
	tokens      []v1Token
}

func v1WriteTokens(w io.Writer, printOpts v1TokenPrintOpt) error {
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
		"Description",
		"Name / Token",
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
			"ID":           t.ID.String(),
			"Description":  t.Description,
			"Name / Token": t.Token,
			"User Name":    t.UserName,
			"User ID":      t.UserID.String(),
			"Permissions":  t.Permissions,
		}
		if printOpts.deleted {
			m["Deleted"] = true
		}
		tabW.Write(m)
	}

	return nil
}

func newV1AuthorizationService() (*authorization.Client, error) {
	httpClient, err := newHTTPClient()
	if err != nil {
		return nil, err
	}

	return &authorization.Client{
		Client: httpClient,
	}, nil
}
