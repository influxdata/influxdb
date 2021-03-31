package main

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/internal"
	"github.com/influxdata/influxdb/v2/http"
	ilogger "github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/spf13/cobra"
	"github.com/tcnksm/go-input"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type userSVCsFn func() (cmdUserDeps, error)

type cmdUserDeps struct {
	userSVC   influxdb.UserService
	orgSvc    influxdb.OrganizationService
	passSVC   influxdb.PasswordsService
	urmSVC    influxdb.UserResourceMappingService
	getPassFn func(*input.UI, bool) string
}

func cmdUser(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	builder := newCmdUserBuilder(newUserSVC, f, opt)
	return builder.cmd()
}

type cmdUserBuilder struct {
	genericCLIOpts
	*globalFlags

	svcFn userSVCsFn

	id       string
	name     string
	password string
	org      organization
}

func newCmdUserBuilder(svcsFn userSVCsFn, f *globalFlags, opt genericCLIOpts) *cmdUserBuilder {
	return &cmdUserBuilder{
		genericCLIOpts: opt,
		globalFlags:    f,
		svcFn:          svcsFn,
	}
}

func (b *cmdUserBuilder) cmd() *cobra.Command {
	cmd := b.genericCLIOpts.newCmd("user", nil, false)
	cmd.Short = "User management commands"
	cmd.Run = seeHelp
	cmd.AddCommand(
		b.cmdCreate(),
		b.cmdDelete(),
		b.cmdFind(),
		b.cmdUpdate(),
		b.cmdPassword(),
	)

	return cmd
}

func (b *cmdUserBuilder) cmdPassword() *cobra.Command {
	cmd := b.newCmd("password", b.cmdPasswordRunEFn)
	cmd.Short = "Update user password"

	cmd.Flags().StringVarP(&b.id, "id", "i", "", "The user ID")
	cmd.Flags().StringVarP(&b.name, "name", "n", "", "The user name")

	return cmd
}

func (b *cmdUserBuilder) cmdPasswordRunEFn(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	dep, err := b.svcFn()
	if err != nil {
		return err
	}

	filter := influxdb.UserFilter{}
	if b.name != "" {
		filter.Name = &b.name
	}
	if b.id != "" {
		id, err := platform.IDFromString(b.id)
		if err != nil {
			return err
		}
		filter.ID = id
	}
	u, err := dep.userSVC.FindUser(ctx, filter)
	if err != nil {
		return err
	}
	ui := &input.UI{
		Writer: b.genericCLIOpts.w,
		Reader: b.genericCLIOpts.in,
	}
	password := dep.getPassFn(ui, true)

	if err = dep.passSVC.SetPassword(ctx, u.ID, password); err != nil {
		return err
	}
	fmt.Fprintln(b.w, "Your password has been successfully updated.")
	return nil
}

func (b *cmdUserBuilder) cmdUpdate() *cobra.Command {
	cmd := b.newCmd("update", b.cmdUpdateRunEFn)
	cmd.Short = "Update user"

	b.registerPrintFlags(cmd)
	cmd.Flags().StringVarP(&b.id, "id", "i", "", "The user ID (required)")
	cmd.Flags().StringVarP(&b.name, "name", "n", "", "The user name")
	cmd.MarkFlagRequired("id")

	return cmd
}

func (b *cmdUserBuilder) cmdUpdateRunEFn(cmd *cobra.Command, args []string) error {
	dep, err := b.svcFn()
	if err != nil {
		return err
	}

	var id platform.ID
	if err := id.DecodeFromString(b.id); err != nil {
		return err
	}

	update := influxdb.UserUpdate{}
	if b.name != "" {
		update.Name = &b.name
	}

	user, err := dep.userSVC.UpdateUser(context.Background(), id, update)
	if err != nil {
		return err
	}

	return b.printUser(userPrintOpts{user: user})
}

func (b *cmdUserBuilder) cmdCreate() *cobra.Command {
	cmd := b.newCmd("create", b.cmdCreateRunEFn)
	cmd.Short = "Create user"

	opts := flagOpts{
		{
			DestP:    &b.name,
			Flag:     "name",
			Short:    'n',
			Desc:     "The user name (required)",
			Required: true,
		},
	}
	opts.mustRegister(b.viper, cmd)

	cmd.Flags().StringVarP(&b.password, "password", "p", "", "The user password")
	b.org.register(b.viper, cmd, false)
	b.registerPrintFlags(cmd)

	return cmd
}

func (b *cmdUserBuilder) cmdCreateRunEFn(*cobra.Command, []string) error {
	ctx := context.Background()
	if err := b.org.validOrgFlags(b.globalFlags); err != nil {
		return err
	}

	if b.password != "" && len(b.password) < internal.MinPasswordLen {
		return internal.ErrPasswordIsTooShort
	}

	conf := &ilogger.Config{
		Level:  zapcore.WarnLevel,
		Format: "auto",
	}
	if b.json {
		conf.Format = "json"
	}

	log, err := conf.New(b.errW)
	if err != nil {
		return err
	}

	dep, err := b.svcFn()
	if err != nil {
		return err
	}

	user := &influxdb.User{
		Name: b.name,
	}

	if err := dep.userSVC.CreateUser(ctx, user); err != nil {
		return err
	}
	if err := b.printUser(userPrintOpts{user: user}); err != nil {
		return err
	}

	orgID, err := b.org.getID(dep.orgSvc)
	if err == nil {
		err = dep.urmSVC.CreateUserResourceMapping(context.Background(), &influxdb.UserResourceMapping{
			UserID:       user.ID,
			UserType:     influxdb.Member,
			ResourceType: influxdb.OrgsResourceType,
			ResourceID:   orgID,
		})
	}
	if err != nil {
		if b.password != "" {
			log.Warn("Hit error before attempting to set password, use `influx user password` to retry", zap.String("user", b.name))
		}
		return fmt.Errorf("failed setting org membership for user %q, use `influx org members add` to retry: %w", b.name, err)
	}

	if b.password != "" {
		if err := dep.passSVC.SetPassword(ctx, user.ID, b.password); err != nil {
			return fmt.Errorf("failed setting password for user %q, use `influx user password` to retry", b.name)
		}
	} else {
		log.Warn("Initial password not set for user, use `influx user password` to set it", zap.String("user", b.name))
	}

	return nil
}

func (b *cmdUserBuilder) cmdFind() *cobra.Command {
	cmd := b.newCmd("list", b.cmdFindRunEFn)
	cmd.Short = "List users"
	cmd.Aliases = []string{"find", "ls"}

	b.registerPrintFlags(cmd)
	cmd.Flags().StringVarP(&b.id, "id", "i", "", "The user ID")
	cmd.Flags().StringVarP(&b.name, "name", "n", "", "The user name")

	return cmd
}

func (b *cmdUserBuilder) cmdFindRunEFn(*cobra.Command, []string) error {
	dep, err := b.svcFn()
	if err != nil {
		return err
	}

	filter := influxdb.UserFilter{}
	if b.name != "" {
		filter.Name = &b.name
	}
	if b.id != "" {
		id, err := platform.IDFromString(b.id)
		if err != nil {
			return err
		}
		filter.ID = id
	}

	users, _, err := dep.userSVC.FindUsers(context.Background(), filter)
	if err != nil {
		return err
	}

	return b.printUser(userPrintOpts{users: users})
}

func (b *cmdUserBuilder) cmdDelete() *cobra.Command {
	cmd := b.newCmd("delete", b.cmdDeleteRunEFn)
	cmd.Short = "Delete user"

	b.registerPrintFlags(cmd)
	cmd.Flags().StringVarP(&b.id, "id", "i", "", "The user ID (required)")
	cmd.MarkFlagRequired("id")

	return cmd
}

func (b *cmdUserBuilder) cmdDeleteRunEFn(cmd *cobra.Command, args []string) error {
	dep, err := b.svcFn()
	if err != nil {
		return err
	}

	var id platform.ID
	if err := id.DecodeFromString(b.id); err != nil {
		return err
	}

	ctx := context.Background()
	u, err := dep.userSVC.FindUserByID(ctx, id)
	if err != nil {
		return err
	}

	if err := dep.userSVC.DeleteUser(ctx, id); err != nil {
		return err
	}

	return b.printUser(userPrintOpts{
		deleted: true,
		user:    u,
	})
}

func (b *cmdUserBuilder) newCmd(use string, runE func(*cobra.Command, []string) error) *cobra.Command {
	cmd := b.genericCLIOpts.newCmd(use, runE, true)
	b.globalFlags.registerFlags(b.viper, cmd)
	return cmd
}

func (b *cmdUserBuilder) registerPrintFlags(cmd *cobra.Command) {
	registerPrintOptions(b.viper, cmd, &b.hideHeaders, &b.json)
}

func (b *cmdUserBuilder) printUser(opt userPrintOpts) error {
	if b.json {
		var v interface{} = opt.users
		if opt.user != nil {
			v = opt.user
		}
		return b.writeJSON(v)
	}

	w := b.newTabWriter()
	defer w.Flush()

	w.HideHeaders(b.hideHeaders)

	headers := []string{"ID", "Name"}
	if opt.deleted {
		headers = append(headers, "Deleted")
	}
	w.WriteHeaders(headers...)

	if opt.user != nil {
		opt.users = append(opt.users, opt.user)
	}

	for _, u := range opt.users {
		m := map[string]interface{}{
			"ID":   u.ID.String(),
			"Name": u.Name,
		}
		if opt.deleted {
			m["Deleted"] = true
		}
		w.Write(m)
	}

	return nil
}

type userPrintOpts struct {
	deleted bool
	user    *influxdb.User
	users   []*influxdb.User
}

func newUserService() (influxdb.UserService, error) {
	client, err := newHTTPClient()
	if err != nil {
		return nil, err
	}
	return &http.UserService{
		Client: client,
	}, nil
}

func newUserSVC() (cmdUserDeps, error) {
	httpClient, err := newHTTPClient()
	if err != nil {
		return cmdUserDeps{}, err
	}
	userSvc := &tenant.UserClientService{Client: httpClient}
	orgSvc := &tenant.OrgClientService{Client: httpClient}
	passSvc := &tenant.PasswordClientService{Client: httpClient}
	urmSvc := &tenant.UserResourceMappingClient{Client: httpClient}
	getPassFn := internal.GetPassword

	return cmdUserDeps{
		userSVC:   userSvc,
		orgSvc:    orgSvc,
		passSVC:   passSvc,
		urmSVC:    urmSvc,
		getPassFn: getPassFn,
	}, nil
}
