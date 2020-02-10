package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/influxdata/influxdb/http"

	"github.com/influxdata/influxdb"
	"github.com/spf13/cobra"
)

type orgSVCFn func() (influxdb.OrganizationService, influxdb.UserResourceMappingService, influxdb.UserService, error)

func cmdOrganization(opts ...genericCLIOptFn) *cobra.Command {
	return newCmdOrgBuilder(newOrgServices, opts...).cmd()
}

type cmdOrgBuilder struct {
	genericCLIOpts

	svcFn orgSVCFn

	description string
	id          string
	memberID    string
	name        string
}

func newCmdOrgBuilder(svcFn orgSVCFn, opts ...genericCLIOptFn) *cmdOrgBuilder {
	opt := genericCLIOpts{
		in: os.Stdin,
		w:  os.Stdout,
	}
	for _, o := range opts {
		o(&opt)
	}

	return &cmdOrgBuilder{
		genericCLIOpts: opt,
		svcFn:          svcFn,
	}
}

func (b *cmdOrgBuilder) cmd() *cobra.Command {
	cmd := b.newCmd("org", nil)
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

	w := b.newTabWriter()
	w.WriteHeaders("ID", "Name")
	w.Write(map[string]interface{}{
		"ID":   org.ID.String(),
		"Name": org.Name,
	})
	w.Flush()

	return nil
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
	opts.mustRegister(cmd)

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

	w := b.newTabWriter()
	w.WriteHeaders("ID", "Name", "Deleted")
	w.Write(map[string]interface{}{
		"ID":      o.ID.String(),
		"Name":    o.Name,
		"Deleted": true,
	})
	w.Flush()

	return nil
}

func (b *cmdOrgBuilder) cmdFind() *cobra.Command {
	cmd := b.newCmd("find", b.findRunEFn)
	cmd.Short = "Find organizations"

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
	opts.mustRegister(cmd)

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

	w := b.newTabWriter()
	w.WriteHeaders("ID", "Name")
	for _, o := range orgs {
		w.Write(map[string]interface{}{
			"ID":   o.ID.String(),
			"Name": o.Name,
		})
	}
	w.Flush()

	return nil
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
	opts.mustRegister(cmd)

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

	w := b.newTabWriter()
	w.WriteHeaders("ID", "Name")
	w.Write(map[string]interface{}{
		"ID":   o.ID.String(),
		"Name": o.Name,
	})
	w.Flush()

	return nil
}

func (b *cmdOrgBuilder) cmdMember() *cobra.Command {
	cmd := b.newCmd("members", nil)
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
	opts.mustRegister(cmd)
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
	return memberList(ctx, b, urmSVC, userSVC, influxdb.UserResourceMappingFilter{
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
	opts.mustRegister(cmd)

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
	opts.mustRegister(cmd)

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

func newOrgServices() (influxdb.OrganizationService, influxdb.UserResourceMappingService, influxdb.UserService, error) {
	if flags.local {
		svc, err := newLocalKVService()
		if err != nil {
			return nil, nil, nil, err
		}
		return svc, svc, svc, nil
	}

	client, err := newHTTPClient()
	if err != nil {
		return nil, nil, nil, err
	}

	orgSVC := &http.OrganizationService{Client: client}
	urmSVC := &http.UserResourceMappingService{Client: client}
	userSVC := &http.UserService{Client: client}

	return orgSVC, urmSVC, userSVC, nil
}

func newOrganizationService() (influxdb.OrganizationService, error) {
	if flags.local {
		return newLocalKVService()
	}

	client, err := newHTTPClient()
	if err != nil {
		return nil, err
	}

	return &http.OrganizationService{
		Client: client,
	}, nil
}

func memberList(ctx context.Context, b *cmdOrgBuilder, urmSVC influxdb.UserResourceMappingService, userSVC influxdb.UserService, f influxdb.UserResourceMappingFilter) error {
	mps, _, err := urmSVC.FindUserResourceMappings(ctx, f)
	if err != nil {
		return fmt.Errorf("failed to find members: %v", err)
	}

	urs := make([]*influxdb.User, len(mps))
	ursC := make(chan struct {
		User  *influxdb.User
		Index int
	})
	errC := make(chan error)
	sem := make(chan struct{}, maxTCPConnections)
	for k, v := range mps {
		sem <- struct{}{}
		go func(k int, v *influxdb.UserResourceMapping) {
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
	for i := 0; i < len(mps); i++ {
		select {
		case <-ctx.Done():
			return &influxdb.Error{
				Msg: "Timeout retrieving user details",
			}
		case err := <-errC:
			return err
		case item := <-ursC:
			urs[item.Index] = item.User
		}
	}

	tw := b.newTabWriter()
	tw.WriteHeaders("ID", "Name", "Status")
	for _, m := range urs {
		tw.Write(map[string]interface{}{
			"ID":     m.ID.String(),
			"Name":   m.Name,
			"Status": string(m.Status),
		})
	}
	tw.Flush()
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
