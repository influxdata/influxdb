package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influx/internal"
	cinternal "github.com/influxdata/influxdb/v2/cmd/internal"
	"github.com/influxdata/influxdb/v2/v1/authorization"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tcnksm/go-input"
)

type v1Token struct {
	ID          platform.ID `json:"id"`
	Description string      `json:"description"`
	Token       string      `json:"token"`
	Status      string      `json:"status"`
	UserName    string      `json:"userName"`
	UserID      platform.ID `json:"userID"`
	Permissions []string    `json:"permissions"`
}

func cmdV1Auth(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := opt.newCmd("auth", nil, false)
	cmd.Aliases = []string{"authorization"}
	cmd.Short = "Authorization management commands for v1 APIs"
	cmd.Run = seeHelp

	cmd.AddCommand(
		v1AuthCreateCmd(f, opt),
		v1AuthDeleteCmd(f, opt),
		v1AuthFindCmd(f, opt),
		v1AuthSetActiveCmd(f, opt),
		v1AuthSetInactiveCmd(f, opt),
		v1AuthSetPasswordCmd(f, opt),
	)

	return cmd
}

var v1AuthCreateFlags struct {
	username    string
	description string
	password    string
	noPassword  bool
	hideHeaders bool
	json        bool
	org         organization

	writeBucketPermissions []string
	readBucketPermissions  []string
}

func v1AuthCreateCmd(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create authorization",
		RunE:  checkSetupRunEMiddleware(&flags)(makeV1AuthorizationCreateE(opt)),
	}

	f.registerFlags(opt.viper, cmd)
	v1AuthCreateFlags.org.register(opt.viper, cmd, false)

	cmd.Flags().StringVar(&v1AuthCreateFlags.username, "username", "", "The username to identify this token")
	_ = cmd.MarkFlagRequired("username")
	cmd.Flags().StringVarP(&v1AuthCreateFlags.description, "description", "d", "", "Token description")
	cmd.Flags().StringVar(&v1AuthCreateFlags.password, "password", "", "The password to set on this token")
	cmd.Flags().BoolVar(&v1AuthCreateFlags.noPassword, "no-password", false, "Don't prompt for a password. You must use v1 auth set-password command before using the token.")
	registerPrintOptions(opt.viper, cmd, &v1AuthCreateFlags.hideHeaders, &v1AuthCreateFlags.json)

	cmd.Flags().StringArrayVarP(&v1AuthCreateFlags.writeBucketPermissions, "write-bucket", "", []string{}, "The bucket id")
	cmd.Flags().StringArrayVarP(&v1AuthCreateFlags.readBucketPermissions, "read-bucket", "", []string{}, "The bucket id")

	return cmd
}

func makeV1AuthorizationCreateE(opt genericCLIOpts) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if err := v1AuthCreateFlags.org.validOrgFlags(&flags); err != nil {
			return err
		}

		if v1AuthCreateFlags.password != "" && v1AuthCreateFlags.noPassword {
			return errors.New("only one of --password and --no-password may be specified")
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

		s, err := newV1AuthorizationService()
		if err != nil {
			return err
		}

		// verify an existing token with the same username doesn't already exist
		filter := influxdb.AuthorizationFilter{
			Token: &v1AuthCreateFlags.username,
		}
		if auth, err := v1FindOneAuthorization(s, filter); err != nil {
			if !errors.Is(err, authorization.ErrAuthNotFound) {
				return err
			}
		} else if auth != nil {
			return fmt.Errorf("authorization with username %q exists", v1AuthCreateFlags.username)
		}

		password := v1AuthCreateFlags.password
		if password == "" && !v1AuthCreateFlags.noPassword {
			ui := &input.UI{
				Writer: opt.w,
				Reader: opt.in,
			}

			password = cinternal.GetPassword(ui, false)
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
				var id platform.ID
				if err := id.DecodeFromString(p); err != nil {
					return fmt.Errorf("invalid bucket ID '%s': %w (did you pass a bucket name instead of an ID?)", p, err)
				}

				p, err := influxdb.NewPermissionAtID(id, bp.action, influxdb.BucketsResourceType, orgID)
				if err != nil {
					return err
				}

				permissions = append(permissions, *p)
			}
		}

		auth := &influxdb.Authorization{
			Token:       v1AuthCreateFlags.username,
			Description: v1AuthCreateFlags.description,
			Permissions: permissions,
			OrgID:       orgID,
		}

		if err := s.CreateAuthorization(context.Background(), auth); err != nil {
			return err
		}

		if password != "" {
			if err := s.SetPassword(context.Background(), auth.ID, password); err != nil {
				_ = s.DeleteAuthorization(context.Background(), auth.ID)
				return fmt.Errorf("error setting password: %w", err)
			}
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
			jsonOut:     v1AuthCreateFlags.json,
			hideHeaders: v1AuthCreateFlags.hideHeaders,
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
}

var v1AuthorizationFindFlags struct {
	org         organization
	lookup      v1AuthLookupFlags
	hideHeaders bool
	json        bool
	user        string
	userID      string
}

func v1AuthFindCmd(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Short:   "List authorizations",
		Aliases: []string{"find", "ls"},
		RunE:    checkSetupRunEMiddleware(&flags)(v1AuthorizationFindF),
	}

	f.registerFlags(opt.viper, cmd)
	v1AuthorizationFindFlags.org.register(opt.viper, cmd, false)
	registerPrintOptions(opt.viper, cmd, &v1AuthorizationFindFlags.hideHeaders, &v1AuthorizationFindFlags.json)
	cmd.Flags().StringVarP(&v1AuthorizationFindFlags.user, "user", "u", "", "The user")
	cmd.Flags().StringVarP(&v1AuthorizationFindFlags.userID, "user-id", "", "", "The user ID")
	v1AuthorizationFindFlags.lookup.register(opt.viper, cmd, false)

	return cmd
}

func v1AuthorizationFindF(cmd *cobra.Command, _ []string) error {
	s, err := newV1AuthorizationService()
	if err != nil {
		return err
	}

	us, err := newUserService()
	if err != nil {
		return err
	}

	var filter influxdb.AuthorizationFilter
	if v1AuthorizationFindFlags.lookup.isSet() {
		if err := v1AuthorizationFindFlags.lookup.validate(); err != nil {
			return err
		}

		filter = v1AuthorizationFindFlags.lookup.makeFilter()
	}

	if v1AuthorizationFindFlags.user != "" {
		filter.User = &v1AuthorizationFindFlags.user
	}
	if v1AuthorizationFindFlags.userID != "" {
		uID, err := platform.IDFromString(v1AuthorizationFindFlags.userID)
		if err != nil {
			return fmt.Errorf("invalid user ID '%s': %w (did you pass a username instead of an ID?)", v1AuthorizationFindFlags.userID, err)
		}
		filter.UserID = uID
	}
	if v1AuthorizationFindFlags.org.name != "" {
		filter.Org = &v1AuthorizationFindFlags.org.name
	}
	if v1AuthorizationFindFlags.org.id != "" {
		oID, err := platform.IDFromString(v1AuthorizationFindFlags.org.id)
		if err != nil {
			return fmt.Errorf("invalid org ID '%s': %w (did you pass an org name instead of an ID?)", v1AuthorizationFindFlags.org.id, err)
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
		jsonOut:     v1AuthorizationFindFlags.json,
		hideHeaders: v1AuthorizationFindFlags.hideHeaders,
		tokens:      tokens,
	})
}

var v1AuthDeleteFlags struct {
	lookup      v1AuthLookupFlags
	hideHeaders bool
	json        bool
}

func v1AuthDeleteCmd(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete authorization",
		RunE:  checkSetupRunEMiddleware(&flags)(v1AuthorizationDeleteF),
	}

	f.registerFlags(opt.viper, cmd)
	registerPrintOptions(opt.viper, cmd, &v1AuthDeleteFlags.hideHeaders, &v1AuthDeleteFlags.json)
	v1AuthDeleteFlags.lookup.required = true
	v1AuthDeleteFlags.lookup.register(opt.viper, cmd, false)

	return cmd
}

func v1AuthorizationDeleteF(cmd *cobra.Command, _ []string) error {
	if err := v1AuthDeleteFlags.lookup.validate(); err != nil {
		return err
	}

	s, err := newV1AuthorizationService()
	if err != nil {
		return err
	}

	us, err := newUserService()
	if err != nil {
		return err
	}

	ctx := context.Background()

	a, err := v1FindOneAuthorization(s, v1AuthDeleteFlags.lookup.makeFilter())
	if err != nil {
		return err
	}

	if err := s.DeleteAuthorization(ctx, a.ID); err != nil {
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
		jsonOut:     v1AuthDeleteFlags.json,
		deleted:     true,
		hideHeaders: v1AuthDeleteFlags.hideHeaders,
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

var v1AuthActivateFlags struct {
	lookup      v1AuthLookupFlags
	hideHeaders bool
	json        bool
}

func v1AuthSetActiveCmd(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-active",
		Short: "Change the status of an authorization to active",
		RunE:  checkSetupRunEMiddleware(&flags)(v1AuthorizationSetActiveF),
	}
	f.registerFlags(opt.viper, cmd)

	registerPrintOptions(opt.viper, cmd, &v1AuthActivateFlags.hideHeaders, &v1AuthActivateFlags.json)
	v1AuthActivateFlags.lookup.required = true
	v1AuthActivateFlags.lookup.register(opt.viper, cmd, false)

	return cmd
}

func v1AuthorizationSetActiveF(cmd *cobra.Command, _ []string) error {
	if err := v1AuthActivateFlags.lookup.validate(); err != nil {
		return err
	}

	s, err := newV1AuthorizationService()
	if err != nil {
		return err
	}

	us, err := newUserService()
	if err != nil {
		return err
	}

	ctx := context.Background()

	a, err := v1FindOneAuthorization(s, v1AuthActivateFlags.lookup.makeFilter())
	if err != nil {
		return err
	}

	a, err = s.UpdateAuthorization(ctx, a.ID, &influxdb.AuthorizationUpdate{
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
		jsonOut:     v1AuthActivateFlags.json,
		hideHeaders: v1AuthActivateFlags.hideHeaders,
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

var v1AuthDeactivateFlags struct {
	lookup      v1AuthLookupFlags
	hideHeaders bool
	json        bool
}

func v1AuthSetInactiveCmd(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-inactive",
		Short: "Change the status of an authorization to inactive",
		RunE:  checkSetupRunEMiddleware(&flags)(v1AuthorizationSetInactiveF),
	}

	f.registerFlags(opt.viper, cmd)
	registerPrintOptions(opt.viper, cmd, &v1AuthDeactivateFlags.hideHeaders, &v1AuthDeactivateFlags.json)
	v1AuthDeactivateFlags.lookup.required = true
	v1AuthDeactivateFlags.lookup.register(opt.viper, cmd, false)

	return cmd
}

func v1AuthorizationSetInactiveF(cmd *cobra.Command, _ []string) error {
	if err := v1AuthDeactivateFlags.lookup.validate(); err != nil {
		return err
	}

	s, err := newV1AuthorizationService()
	if err != nil {
		return err
	}

	us, err := newUserService()
	if err != nil {
		return err
	}

	ctx := context.Background()

	a, err := v1FindOneAuthorization(s, v1AuthDeactivateFlags.lookup.makeFilter())
	if err != nil {
		return err
	}

	a, err = s.UpdateAuthorization(ctx, a.ID, &influxdb.AuthorizationUpdate{
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
		jsonOut:     v1AuthDeactivateFlags.json,
		hideHeaders: v1AuthDeactivateFlags.hideHeaders,
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

var (
	errMultipleMatchingAuthorizations = errors.New("multiple authorizations found")
)

func v1FindOneAuthorization(s *authorization.Client, filter influxdb.AuthorizationFilter) (*influxdb.Authorization, error) {
	authorizations, _, err := s.FindAuthorizations(context.Background(), filter)
	if err != nil {
		if err.Error() == authorization.ErrAuthNotFound.Msg {
			return nil, authorization.ErrAuthNotFound
		}

		return nil, err
	}

	if len(authorizations) > 1 {
		return nil, errMultipleMatchingAuthorizations
	}

	return authorizations[0], nil
}

type v1AuthLookupFlags struct {
	id       platform.ID
	username string
	required bool // required when set to true determines whether validate expects either id or username to be set
}

func (f *v1AuthLookupFlags) register(v *viper.Viper, cmd *cobra.Command, persistent bool) {
	opts := flagOpts{
		{
			DestP:      &f.id,
			Flag:       "id",
			Desc:       "The ID of the authorization",
			Persistent: persistent,
		},
		{
			DestP:      &f.username,
			Flag:       "username",
			Desc:       "The username of the authorization",
			Persistent: persistent,
		},
	}
	opts.mustRegister(v, cmd)
}

func (f *v1AuthLookupFlags) validate() error {
	switch {
	case f.id.Valid() && f.username != "":
		return errors.New("specify id or username, not both")
	case f.required && (!f.id.Valid() && f.username == ""):
		return errors.New("id or username required")
	default:
		return nil
	}
}

func (f *v1AuthLookupFlags) isSet() bool {
	return f.id.Valid() || f.username != ""
}

func (f *v1AuthLookupFlags) makeFilter() influxdb.AuthorizationFilter {
	if f.id.Valid() {
		return influxdb.AuthorizationFilter{ID: &f.id}
	}
	if f.username != "" {
		return influxdb.AuthorizationFilter{Token: &f.username}
	}
	return influxdb.AuthorizationFilter{}
}

var v1AuthSetPasswordFlags struct {
	lookup   v1AuthLookupFlags
	password string
}

func v1AuthSetPasswordCmd(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-password",
		Short: "Set a password for an existing authorization",
		RunE:  checkSetupRunEMiddleware(&flags)(makeV1AuthorizationSetPasswordF(opt)),
	}

	f.registerFlags(opt.viper, cmd)
	v1AuthSetPasswordFlags.lookup.register(opt.viper, cmd, false)
	cmd.Flags().StringVar(&v1AuthSetPasswordFlags.password, "password", "", "Password to set on the authorization")

	return cmd
}

func makeV1AuthorizationSetPasswordF(opt genericCLIOpts) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if err := v1AuthSetPasswordFlags.lookup.validate(); err != nil {
			return err
		}

		s, err := newV1AuthorizationService()
		if err != nil {
			return err
		}

		auth, err := v1FindOneAuthorization(s, v1AuthSetPasswordFlags.lookup.makeFilter())
		if err != nil {
			return err
		}

		password := v1AuthSetPasswordFlags.password
		if password == "" {
			ui := &input.UI{
				Writer: opt.w,
				Reader: opt.in,
			}
			password = cinternal.GetPassword(ui, false)
		}

		if err := s.SetPassword(context.Background(), auth.ID, password); err != nil {
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
