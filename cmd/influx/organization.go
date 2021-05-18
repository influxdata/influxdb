package main

import (
	"context"
	"fmt"
	"io"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/spf13/cobra"
)

type orgSVCFn func() (influxdb.OrganizationService, influxdb.UserResourceMappingService, influxdb.UserService, error)

func cmdOrganization(f *globalFlags, opts genericCLIOpts) *cobra.Command {
	builder := newCmdOrgBuilder(newOrgServices, f, opts)
	return builder.cmd()
}

type cmdOrgBuilder struct {
	genericCLIOpts
	*globalFlags

	svcFn orgSVCFn

	json        bool
	hideHeaders bool
	description string
	id          string
	memberID    string
	name        string
}

func newCmdOrgBuilder(svcFn orgSVCFn, f *globalFlags, opts genericCLIOpts) *cmdOrgBuilder {
	return &cmdOrgBuilder{
		genericCLIOpts: opts,
		globalFlags:    f,
		svcFn:          svcFn,
	}
}

func (b *cmdOrgBuilder) cmd() *cobra.Command {
	cmd := b.genericCLIOpts.newCmd("org", nil, false)
	cmd.Aliases = []string{"organization"}
	cmd.Short = "Organization management commands"
	cmd.Run = seeHelp

	cmd.AddCommand(
		b.cmdCreate(),
		b.cmdDelete(),
		b.cmdFind(),
		b.cmdMember(),
		b.cmdUpdate(),
	)

	return cmd
}

func (b *cmdOrgBuilder) cmdCreate() *cobra.Command {
	cmd := b.newCmd("create", b.createRunEFn)
	cmd.Short = "Create organization"

	b.registerPrintFlags(cmd)
	cmd.Flags().StringVarP(&b.name, "name", "n", "", "The name of organization that will be created")
	cmd.MarkFlagRequired("name")
	cmd.Flags().StringVarP(&b.description, "description", "d", "", "The description of the organization that will be created")

	return cmd
}

func (b *cmdOrgBuilder) createRunEFn(cmd *cobra.Command, args []string) error {
	orgSvc, _, _, err := b.svcFn()
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	org := &influxdb.Organization{
		Name:        b.name,
		Description: b.description,
	}

	if err := orgSvc.CreateOrganization(context.Background(), org); err != nil {
		return fmt.Errorf("failed to create organization: %v", err)
	}

	return b.printOrg(orgPrintOpt{org: org})
}

func (b *cmdOrgBuilder) cmdDelete() *cobra.Command {
	cmd := b.newCmd("delete", b.deleteRunEFn)
	cmd.Short = "Delete organization"

	opts := flagOpts{
		{
			DestP:  &b.id,
			Flag:   "id",
			Short:  'i',
			EnvVar: "ORG_ID",
			Desc:   "The organization ID",
		},
	}
	opts.mustRegister(b.viper, cmd)
	b.registerPrintFlags(cmd)

	return cmd
}

func (b *cmdOrgBuilder) deleteRunEFn(cmd *cobra.Command, args []string) error {
	orgSvc, _, _, err := b.svcFn()
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	var id influxdb.ID
	if err := id.DecodeFromString(b.id); err != nil {
		return fmt.Errorf("failed to decode org id %s: %v", b.id, err)
	}

	ctx := context.TODO()
	o, err := orgSvc.FindOrganizationByID(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to find org with id %q: %v", id, err)
	}

	if err = orgSvc.DeleteOrganization(ctx, id); err != nil {
		return fmt.Errorf("failed to delete org with id %q: %v", id, err)
	}

	return b.printOrg(orgPrintOpt{
		deleted: true,
		org:     o,
	})
}

func (b *cmdOrgBuilder) cmdFind() *cobra.Command {
	cmd := b.newCmd("list", b.findRunEFn)
	cmd.Short = "List organizations"
	cmd.Aliases = []string{"find", "ls"}

	opts := flagOpts{
		{
			DestP:  &b.name,
			Flag:   "name",
			Short:  'n',
			EnvVar: "ORG",
			Desc:   "The organization name",
		},
		{
			DestP:  &b.id,
			Flag:   "id",
			Short:  'i',
			EnvVar: "ORG_ID",
			Desc:   "The organization ID",
		},
	}
	opts.mustRegister(b.viper, cmd)
	b.registerPrintFlags(cmd)

	return cmd
}

func (b *cmdOrgBuilder) findRunEFn(cmd *cobra.Command, args []string) error {
	orgSvc, _, _, err := b.svcFn()
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	filter := influxdb.OrganizationFilter{}
	if b.name != "" {
		filter.Name = &b.name
	}

	if b.id != "" {
		id, err := influxdb.IDFromString(b.id)
		if err != nil {
			return fmt.Errorf("failed to decode org id %s: %v", b.id, err)
		}
		filter.ID = id
	}

	orgs, _, err := orgSvc.FindOrganizations(context.Background(), filter)
	if err != nil {
		return fmt.Errorf("failed find orgs: %v", err)
	}

	return b.printOrg(orgPrintOpt{orgs: orgs})
}

func (b *cmdOrgBuilder) cmdUpdate() *cobra.Command {
	cmd := b.newCmd("update", b.updateRunEFn)
	cmd.Short = "Update organization"

	opts := flagOpts{
		{
			DestP:    &b.id,
			Flag:     "id",
			Short:    'i',
			EnvVar:   "ORG_ID",
			Desc:     "The organization ID (required)",
			Required: true,
		},
		{
			DestP:  &b.name,
			Flag:   "name",
			Short:  'n',
			EnvVar: "ORG",
			Desc:   "The organization name",
		},
		{
			DestP:  &b.description,
			Flag:   "description",
			Short:  'd',
			EnvVar: "ORG_DESCRIPTION",
			Desc:   "The organization name",
		},
	}
	opts.mustRegister(b.viper, cmd)
	b.registerPrintFlags(cmd)

	return cmd
}

func (b *cmdOrgBuilder) updateRunEFn(cmd *cobra.Command, args []string) error {
	orgSvc, _, _, err := b.svcFn()
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	var id influxdb.ID
	if err := id.DecodeFromString(b.id); err != nil {
		return fmt.Errorf("failed to decode org id %s: %v", b.id, err)
	}

	update := influxdb.OrganizationUpdate{}
	if b.name != "" {
		update.Name = &b.name
	}
	if b.description != "" {
		update.Description = &b.description
	}

	o, err := orgSvc.UpdateOrganization(context.Background(), id, update)
	if err != nil {
		return fmt.Errorf("failed to update org: %v", err)
	}

	return b.printOrg(orgPrintOpt{org: o})
}

func (b *cmdOrgBuilder) printOrg(opts orgPrintOpt) error {
	if b.json {
		var v interface{} = opts.orgs
		if opts.org != nil {
			v = opts.org
		}
		return b.writeJSON(v)
	}

	w := b.newTabWriter()
	defer w.Flush()

	w.HideHeaders(b.hideHeaders)

	headers := []string{"ID", "Name"}
	if opts.deleted {
		headers = append(headers, "Deleted")
	}
	w.WriteHeaders(headers...)

	if opts.org != nil {
		opts.orgs = append(opts.orgs, opts.org)
	}

	for _, o := range opts.orgs {
		m := map[string]interface{}{
			"ID":   o.ID.String(),
			"Name": o.Name,
		}
		if opts.deleted {
			m["Deleted"] = true
		}
		w.Write(m)
	}

	return nil
}

func (b *cmdOrgBuilder) cmdMember() *cobra.Command {
	cmd := b.genericCLIOpts.newCmd("members", nil, false)
	cmd.Short = "Organization membership commands"
	cmd.Run = seeHelp

	cmd.AddCommand(
		b.cmdMemberAdd(),
		b.cmdMemberList(),
		b.cmdMemberRemove(),
	)

	return cmd
}

func (b *cmdOrgBuilder) cmdMemberList() *cobra.Command {
	cmd := b.newCmd("list", b.memberListRunEFn)
	cmd.Short = "List organization members"
	cmd.Aliases = []string{"find", "ls"}

	opts := flagOpts{
		{
			DestP:  &b.name,
			Flag:   "name",
			Short:  'n',
			EnvVar: "ORG",
			Desc:   "The organization name",
		},
		{
			DestP:  &b.id,
			Flag:   "id",
			Short:  'i',
			EnvVar: "ORG_ID",
			Desc:   "The organization ID",
		},
	}
	opts.mustRegister(b.viper, cmd)
	b.registerPrintFlags(cmd)
	return cmd
}

func (b *cmdOrgBuilder) memberListRunEFn(cmd *cobra.Command, args []string) error {
	orgSvc, urmSVC, userSVC, err := b.svcFn()
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	if b.id == "" && b.name == "" {
		return fmt.Errorf("must specify exactly one of id and name")
	}

	var filter influxdb.OrganizationFilter
	if b.name != "" {
		filter.Name = &b.name
	}

	if b.id != "" {
		var fID influxdb.ID
		err := fID.DecodeFromString(b.id)
		if err != nil {
			return fmt.Errorf("failed to decode org id %s: %v", b.id, err)
		}
		filter.ID = &fID
	}

	organization, err := orgSvc.FindOrganization(context.Background(), filter)
	if err != nil {
		return fmt.Errorf("failed to find org: %v", err)
	}

	ctx := context.Background()
	return b.memberList(ctx, urmSVC, userSVC, influxdb.UserResourceMappingFilter{
		ResourceType: influxdb.OrgsResourceType,
		ResourceID:   organization.ID,
		UserType:     influxdb.Member,
	})
}

func (b *cmdOrgBuilder) cmdMemberAdd() *cobra.Command {
	cmd := b.newCmd("add", b.memberAddRunEFn)
	cmd.Short = "Add organization member"

	cmd.Flags().StringVarP(&b.memberID, "member", "m", "", "The member ID")
	cmd.MarkFlagRequired("member")

	opts := flagOpts{
		{
			DestP:  &b.name,
			Flag:   "name",
			Short:  'n',
			EnvVar: "ORG",
			Desc:   "The organization name",
		},
		{
			DestP:  &b.id,
			Flag:   "id",
			Short:  'i',
			EnvVar: "ORG_ID",
			Desc:   "The organization ID",
		},
	}
	opts.mustRegister(b.viper, cmd)

	return cmd
}

func (b *cmdOrgBuilder) memberAddRunEFn(cmd *cobra.Command, args []string) error {
	if b.id == "" && b.name == "" {
		return fmt.Errorf("must specify exactly one of id and name")
	}
	if b.id != "" && b.name != "" {
		return fmt.Errorf("must specify exactly one of id and name")
	}

	orgSvc, urmSVC, _, err := b.svcFn()
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	var filter influxdb.OrganizationFilter
	if b.name != "" {
		filter.Name = &b.name
	}

	if b.id != "" {
		var fID influxdb.ID
		err := fID.DecodeFromString(b.id)
		if err != nil {
			return fmt.Errorf("failed to decode org id %s: %v", b.id, err)
		}
		filter.ID = &fID
	}

	ctx := context.Background()
	organization, err := orgSvc.FindOrganization(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to find org: %v", err)
	}

	var memberID influxdb.ID
	err = memberID.DecodeFromString(b.memberID)
	if err != nil {
		return fmt.Errorf("failed to decode member id %s: %v", b.memberID, err)
	}

	return addMember(ctx, b.w, urmSVC, influxdb.UserResourceMapping{
		ResourceID:   organization.ID,
		ResourceType: influxdb.OrgsResourceType,
		MappingType:  influxdb.UserMappingType,
		UserID:       memberID,
		UserType:     influxdb.Member,
	})
}

func (b *cmdOrgBuilder) cmdMemberRemove() *cobra.Command {
	cmd := b.newCmd("remove", b.membersRemoveRunEFn)
	cmd.Short = "Remove organization member"

	opts := flagOpts{
		{
			DestP:  &b.name,
			Flag:   "name",
			Short:  'n',
			EnvVar: "ORG",
			Desc:   "The organization name",
		},
		{
			DestP:  &b.id,
			Flag:   "id",
			Short:  'i',
			EnvVar: "ORG_ID",
			Desc:   "The organization ID",
		},
	}
	opts.mustRegister(b.viper, cmd)

	cmd.Flags().StringVarP(&b.memberID, "member", "m", "", "The member ID")
	cmd.MarkFlagRequired("member")

	return cmd
}

func (b *cmdOrgBuilder) membersRemoveRunEFn(cmd *cobra.Command, args []string) error {
	if b.id == "" && b.name == "" {
		return fmt.Errorf("must specify exactly one of id and name")
	}

	if b.id != "" && b.name != "" {
		return fmt.Errorf("must specify exactly one of id and name")
	}

	orgSvc, urmSVC, _, err := b.svcFn()
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	var filter influxdb.OrganizationFilter
	if b.name != "" {
		filter.Name = &b.name
	}

	if b.id != "" {
		var fID influxdb.ID
		err := fID.DecodeFromString(b.id)
		if err != nil {
			return fmt.Errorf("failed to decode org id %s: %v", b.id, err)
		}
		filter.ID = &fID
	}

	ctx := context.Background()
	organization, err := orgSvc.FindOrganization(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to find organization: %v", err)
	}

	var memberID influxdb.ID
	err = memberID.DecodeFromString(b.memberID)
	if err != nil {
		return fmt.Errorf("failed to decode member id %s: %v", b.memberID, err)
	}

	return removeMember(ctx, b.w, urmSVC, organization.ID, memberID)
}

func (b *cmdOrgBuilder) newCmd(use string, runE func(*cobra.Command, []string) error) *cobra.Command {
	cmd := b.genericCLIOpts.newCmd(use, runE, true)
	b.globalFlags.registerFlags(b.viper, cmd)
	return cmd
}

func (b *cmdOrgBuilder) registerPrintFlags(cmd *cobra.Command) {
	registerPrintOptions(b.viper, cmd, &b.hideHeaders, &b.json)
}

func newOrgServices() (influxdb.OrganizationService, influxdb.UserResourceMappingService, influxdb.UserService, error) {
	client, err := newHTTPClient()
	if err != nil {
		return nil, nil, nil, err
	}

	orgSVC := &tenant.OrgClientService{Client: client}
	urmSVC := &tenant.UserResourceMappingClient{Client: client}
	userSVC := &tenant.UserClientService{Client: client}

	return orgSVC, urmSVC, userSVC, nil
}

func newOrganizationService() (influxdb.OrganizationService, error) {
	client, err := newHTTPClient()
	if err != nil {
		return nil, err
	}

	return &tenant.OrgClientService{
		Client: client,
	}, nil
}

func (b *cmdOrgBuilder) memberList(ctx context.Context, urmSVC influxdb.UserResourceMappingService, userSVC influxdb.UserService, f influxdb.UserResourceMappingFilter) error {
	mappings, _, err := urmSVC.FindUserResourceMappings(ctx, f)
	if err != nil {
		return fmt.Errorf("failed to find members: %v", err)
	}

	var (
		ursC = make(chan struct {
			User  *influxdb.User
			Index int
		}, maxTCPConnections)
		errC = make(chan error, maxTCPConnections)
		sem  = make(chan struct{}, maxTCPConnections)
	)
	for k, v := range mappings {
		go func(k int, v *influxdb.UserResourceMapping) {
			sem <- struct{}{}
			defer func() { <-sem }()
			usr, err := userSVC.FindUserByID(ctx, v.UserID)
			if err != nil {
				errC <- fmt.Errorf("failed to retrieve user details: %v", err)
				return
			}
			ursC <- struct {
				User  *influxdb.User
				Index int
			}{
				User:  usr,
				Index: k,
			}
		}(k, v)
	}

	users := make([]*influxdb.User, len(mappings))
	for i := 0; i < len(mappings); i++ {
		select {
		case <-ctx.Done():
			return &influxdb.Error{
				Msg: "Timeout retrieving user details",
			}
		case err := <-errC:
			return err
		case item := <-ursC:
			users[item.Index] = item.User
		}
	}

	if b.json {
		return b.writeJSON(users)
	}

	tw := b.newTabWriter()
	defer tw.Flush()

	tw.HideHeaders(b.hideHeaders)

	tw.WriteHeaders("ID", "Name", "User Type", "Status")
	for idx, m := range users {
		tw.Write(map[string]interface{}{
			"ID":        m.ID.String(),
			"User Name": m.Name,
			"User Type": string(mappings[idx].UserType),
			"Status":    string(m.Status),
		})
	}

	return nil
}

func addMember(ctx context.Context, w io.Writer, urmSVC influxdb.UserResourceMappingService, urm influxdb.UserResourceMapping) error {
	if err := urmSVC.CreateUserResourceMapping(ctx, &urm); err != nil {
		return fmt.Errorf("failed to add member: %v", err)
	}
	_, err := fmt.Fprintf(w, "user %s has been added as a %s of %s: %s\n", urm.UserID, urm.UserType, urm.ResourceType, urm.ResourceID)
	return err
}

func removeMember(ctx context.Context, w io.Writer, urmSVC influxdb.UserResourceMappingService, resourceID, userID influxdb.ID) error {
	if err := urmSVC.DeleteUserResourceMapping(ctx, resourceID, userID); err != nil {
		return fmt.Errorf("failed to remove member: %v", err)
	}
	_, err := fmt.Fprintf(w, "userID %s has been removed from ResourceID %s\n", userID, resourceID)
	return err
}

type orgPrintOpt struct {
	deleted bool
	org     *influxdb.Organization
	orgs    []*influxdb.Organization
}
