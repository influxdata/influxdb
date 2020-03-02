package main

import (
	"context"
	"fmt"
	"os"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influx/internal"
	"github.com/influxdata/influxdb/http"
	"github.com/spf13/cobra"
)

func cmdOrganization() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "org",
		Aliases: []string{"organization"},
		Short:   "Organization management commands",
		Run:     seeHelp,
	}

	cmd.AddCommand(
		orgCreateCmd(),
		orgDeleteCmd(),
		orgFindCmd(),
		orgMembersCmd(),
		orgUpdateCmd(),
	)

	return cmd
}

var organizationCreateFlags struct {
	name string
}

func orgCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create organization",
		RunE:  wrapCheckSetup(organizationCreateF),
	}

	cmd.Flags().StringVarP(&organizationCreateFlags.name, "name", "n", "", "The name of organization that will be created")
	cmd.MarkFlagRequired("name")

	return cmd
}

func newOrganizationService() (platform.OrganizationService, error) {
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

func organizationCreateF(cmd *cobra.Command, args []string) error {
	orgSvc, err := newOrganizationService()
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	o := &platform.Organization{
		Name: organizationCreateFlags.name,
	}

	if err := orgSvc.CreateOrganization(context.Background(), o); err != nil {
		return fmt.Errorf("failed to create organization: %v", err)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
	)
	w.Write(map[string]interface{}{
		"ID":   o.ID.String(),
		"Name": o.Name,
	})
	w.Flush()

	return nil
}

var organizationFindFlags struct {
	name string
	id   string
}

func orgFindCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "find",
		Short: "Find organizations",
		RunE:  wrapCheckSetup(organizationFindF),
	}

	opts := flagOpts{
		{
			DestP:  &organizationFindFlags.name,
			Flag:   "name",
			Short:  'n',
			EnvVar: "ORG",
			Desc:   "The organization name",
		},
		{
			DestP:  &organizationFindFlags.id,
			Flag:   "id",
			Short:  'i',
			EnvVar: "ORG_ID",
			Desc:   "The organization ID",
		},
	}
	opts.mustRegister(cmd)

	return cmd
}

func organizationFindF(cmd *cobra.Command, args []string) error {
	orgSvc, err := newOrganizationService()
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	filter := platform.OrganizationFilter{}
	if organizationFindFlags.name != "" {
		filter.Name = &organizationFindFlags.name
	}

	if organizationFindFlags.id != "" {
		id, err := platform.IDFromString(organizationFindFlags.id)
		if err != nil {
			return fmt.Errorf("failed to decode org id %s: %v", organizationFindFlags.id, err)
		}
		filter.ID = id
	}

	orgs, _, err := orgSvc.FindOrganizations(context.Background(), filter)
	if err != nil {
		return fmt.Errorf("failed find orgs: %v", err)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
	)
	for _, o := range orgs {
		w.Write(map[string]interface{}{
			"ID":   o.ID.String(),
			"Name": o.Name,
		})
	}
	w.Flush()

	return nil
}

var organizationUpdateFlags struct {
	id   string
	name string
}

func orgUpdateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "Update organization",
		RunE:  wrapCheckSetup(organizationUpdateF),
	}

	opts := flagOpts{
		{
			DestP:    &organizationUpdateFlags.id,
			Flag:     "id",
			Short:    'i',
			EnvVar:   "ORG_ID",
			Desc:     "The organization ID (required)",
			Required: true,
		},
		{
			DestP:  &organizationUpdateFlags.name,
			Flag:   "name",
			Short:  'n',
			EnvVar: "ORG",
			Desc:   "The organization name",
		},
	}
	opts.mustRegister(cmd)

	return cmd
}

func organizationUpdateF(cmd *cobra.Command, args []string) error {
	orgSvc, err := newOrganizationService()
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	var id platform.ID
	if err := id.DecodeFromString(organizationUpdateFlags.id); err != nil {
		return fmt.Errorf("failed to decode org id %s: %v", organizationUpdateFlags.id, err)
	}

	update := platform.OrganizationUpdate{}
	if organizationUpdateFlags.name != "" {
		update.Name = &organizationUpdateFlags.name
	}

	o, err := orgSvc.UpdateOrganization(context.Background(), id, update)
	if err != nil {
		return fmt.Errorf("failed to update org: %v", err)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
	)
	w.Write(map[string]interface{}{
		"ID":   o.ID.String(),
		"Name": o.Name,
	})
	w.Flush()

	return nil
}

var organizationDeleteFlags struct {
	id string
}

func organizationDeleteF(cmd *cobra.Command, args []string) error {
	orgSvc, err := newOrganizationService()
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	var id platform.ID
	if err := id.DecodeFromString(organizationDeleteFlags.id); err != nil {
		return fmt.Errorf("failed to decode org id %s: %v", organizationDeleteFlags.id, err)
	}

	ctx := context.TODO()
	o, err := orgSvc.FindOrganizationByID(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to find org with id %q: %v", id, err)
	}

	if err = orgSvc.DeleteOrganization(ctx, id); err != nil {
		return fmt.Errorf("failed to delete org with id %q: %v", id, err)
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
		"Deleted",
	)
	w.Write(map[string]interface{}{
		"ID":      o.ID.String(),
		"Name":    o.Name,
		"Deleted": true,
	})
	w.Flush()

	return nil
}

func orgDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete organization",
		RunE:  wrapCheckSetup(organizationDeleteF),
	}

	opts := flagOpts{
		{
			DestP:  &organizationFindFlags.id,
			Flag:   "id",
			Short:  'i',
			EnvVar: "ORG_ID",
			Desc:   "The organization ID",
		},
	}
	opts.mustRegister(cmd)

	return cmd
}

var orgMemberFlags struct {
	name     string
	id       string
	memberID string
}

func orgMembersCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "members",
		Short: "Organization membership commands",
		Run:   seeHelp,
	}

	opts := flagOpts{
		{
			DestP:      &orgMemberFlags.name,
			Flag:       "name",
			Short:      'n',
			EnvVar:     "ORG",
			Desc:       "The organization name",
			Persistent: true,
		},
		{
			DestP:      &orgMemberFlags.id,
			Flag:       "id",
			Short:      'i',
			EnvVar:     "ORG_ID",
			Desc:       "The organization ID",
			Persistent: true,
		},
	}
	opts.mustRegister(cmd)

	cmd.AddCommand(
		orgMembersAddCmd(),
		orgMembersListCmd(),
		orgMembersRemoveCmd(),
	)

	return cmd
}

func organizationMembersListF(cmd *cobra.Command, args []string) error {
	orgSvc, err := newOrganizationService()
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	if orgMemberFlags.id == "" && orgMemberFlags.name == "" {
		return fmt.Errorf("must specify exactly one of id and name")
	}

	filter := platform.OrganizationFilter{}
	if orgMemberFlags.name != "" {
		filter.Name = &orgMemberFlags.name
	}

	if orgMemberFlags.id != "" {
		var fID platform.ID
		err := fID.DecodeFromString(orgMemberFlags.id)
		if err != nil {
			return fmt.Errorf("failed to decode org id %s: %v", orgMemberFlags.id, err)
		}
		filter.ID = &fID
	}

	organization, err := orgSvc.FindOrganization(context.Background(), filter)
	if err != nil {
		return fmt.Errorf("failed to find org: %v", err)
	}

	return membersListF(context.Background(), platform.UserResourceMappingFilter{
		ResourceType: platform.OrgsResourceType,
		ResourceID:   organization.ID,
		UserType:     platform.Member,
	})
}

func orgMembersListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List organization members",
		RunE:  wrapCheckSetup(organizationMembersListF),
	}
}

func organizationMembersAddF(cmd *cobra.Command, args []string) error {
	if orgMemberFlags.id == "" && orgMemberFlags.name == "" {
		return fmt.Errorf("must specify exactly one of id and name")
	}

	if orgMemberFlags.id != "" && orgMemberFlags.name != "" {
		return fmt.Errorf("must specify exactly one of id and name")
	}

	orgSvc, err := newOrganizationService()
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	filter := platform.OrganizationFilter{}
	if orgMemberFlags.name != "" {
		filter.Name = &orgMemberFlags.name
	}

	if orgMemberFlags.id != "" {
		var fID platform.ID
		err := fID.DecodeFromString(orgMemberFlags.id)
		if err != nil {
			return fmt.Errorf("failed to decode org id %s: %v", orgMemberFlags.id, err)
		}
		filter.ID = &fID
	}

	ctx := context.Background()
	organization, err := orgSvc.FindOrganization(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to find org: %v", err)
	}

	var memberID platform.ID
	err = memberID.DecodeFromString(orgMemberFlags.memberID)
	if err != nil {
		return fmt.Errorf("failed to decode member id %s: %v", orgMemberFlags.memberID, err)
	}

	return membersAddF(ctx, platform.UserResourceMapping{
		ResourceID:   organization.ID,
		ResourceType: platform.OrgsResourceType,
		MappingType:  platform.UserMappingType,
		UserID:       memberID,
		UserType:     platform.Member,
	})
}

func orgMembersAddCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "add",
		Short: "Add organization member",
		RunE:  wrapCheckSetup(organizationMembersAddF),
	}

	cmd.Flags().StringVarP(&orgMemberFlags.memberID, "member", "o", "", "The member ID")
	cmd.MarkFlagRequired("member")

	return cmd
}

func organizationMembersRemoveF(cmd *cobra.Command, args []string) error {
	if orgMemberFlags.id == "" && orgMemberFlags.name == "" {
		return fmt.Errorf("must specify exactly one of id and name")
	}

	if orgMemberFlags.id != "" && orgMemberFlags.name != "" {
		return fmt.Errorf("must specify exactly one of id and name")
	}

	orgSvc, err := newOrganizationService()
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	filter := platform.OrganizationFilter{}
	if orgMemberFlags.name != "" {
		filter.Name = &orgMemberFlags.name
	}

	if orgMemberFlags.id != "" {
		var fID platform.ID
		err := fID.DecodeFromString(orgMemberFlags.id)
		if err != nil {
			return fmt.Errorf("failed to decode org id %s: %v", orgMemberFlags.id, err)
		}
		filter.ID = &fID
	}

	ctx := context.Background()
	organization, err := orgSvc.FindOrganization(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to find organization: %v", err)
	}

	var memberID platform.ID
	err = memberID.DecodeFromString(orgMemberFlags.memberID)
	if err != nil {
		return fmt.Errorf("failed to decode member id %s: %v", orgMemberFlags.memberID, err)
	}

	return membersRemoveF(ctx, organization.ID, memberID)
}

func orgMembersRemoveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove",
		Short: "Remove organization member",
		RunE:  wrapCheckSetup(organizationMembersRemoveF),
	}

	cmd.Flags().StringVarP(&orgMemberFlags.memberID, "member", "o", "", "The member ID")
	cmd.MarkFlagRequired("member")

	return cmd
}
