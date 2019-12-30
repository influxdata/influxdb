package main

import (
	"context"
	"fmt"
	"os"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influx/internal"
	"github.com/influxdata/influxdb/http"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func organizationCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "org",
		Aliases: []string{"organization"},
		Short:   "Organization management commands",
		Run:     organizationF,
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

func organizationF(cmd *cobra.Command, args []string) {
	cmd.Usage()
}

// Create Command
type OrganizationCreateFlags struct {
	name string
}

var organizationCreateFlags OrganizationCreateFlags

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

// Find Command
type OrganizationFindFlags struct {
	name string
	id   string
}

var organizationFindFlags OrganizationFindFlags

func orgFindCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "find",
		Short: "Find organizations",
		RunE:  wrapCheckSetup(organizationFindF),
	}

	cmd.Flags().StringVarP(&organizationFindFlags.name, "name", "n", "", "The organization name")
	viper.BindEnv("ORG")
	if h := viper.GetString("ORG"); h != "" {
		organizationFindFlags.name = h
	}
	cmd.Flags().StringVarP(&organizationFindFlags.id, "id", "i", "", "The organization ID")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		organizationFindFlags.id = h
	}

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

// Update Command
type OrganizationUpdateFlags struct {
	id   string
	name string
}

var organizationUpdateFlags OrganizationUpdateFlags

func orgUpdateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "Update organization",
		RunE:  wrapCheckSetup(organizationUpdateF),
	}

	cmd.Flags().StringVarP(&organizationUpdateFlags.id, "id", "i", "", "The organization ID (required)")
	cmd.MarkFlagRequired("id")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		organizationUpdateFlags.id = h
	}

	cmd.Flags().StringVarP(&organizationUpdateFlags.name, "name", "n", "", "The organization name")
	viper.BindEnv("ORG")
	if h := viper.GetString("ORG"); h != "" {
		organizationUpdateFlags.name = h
	}

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

// OrganizationDeleteFlags contains the flag of the org delete command
type OrganizationDeleteFlags struct {
	id string
}

var organizationDeleteFlags OrganizationDeleteFlags

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

	cmd.Flags().StringVarP(&organizationDeleteFlags.id, "id", "i", "", "The organization ID (required)")
	cmd.MarkFlagRequired("id")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		organizationUpdateFlags.id = h
	}

	return cmd
}

func orgMembersCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "members",
		Short: "Organization membership commands",
		Run:   organizationF,
	}

	cmd.AddCommand(
		orgMembersAddCmd(),
		orgMembersListCmd(),
		orgMembersRemoveCmd(),
	)

	return cmd
}

// List Members
type OrganizationMembersListFlags struct {
	name string
	id   string
}

var organizationMembersListFlags OrganizationMembersListFlags

func organizationMembersListF(cmd *cobra.Command, args []string) error {
	orgSvc, err := newOrganizationService()
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	if organizationMembersListFlags.id == "" && organizationMembersListFlags.name == "" {
		return fmt.Errorf("must specify exactly one of id and name")
	}

	filter := platform.OrganizationFilter{}
	if organizationMembersListFlags.name != "" {
		filter.Name = &organizationMembersListFlags.name
	}

	if organizationMembersListFlags.id != "" {
		var fID platform.ID
		err := fID.DecodeFromString(organizationMembersListFlags.id)
		if err != nil {
			return fmt.Errorf("failed to decode org id %s: %v", organizationMembersListFlags.id, err)
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
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List organization members",
		RunE:  wrapCheckSetup(organizationMembersListF),
	}

	cmd.Flags().StringVarP(&organizationMembersListFlags.id, "id", "i", "", "The organization ID")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		organizationMembersListFlags.id = h
	}
	cmd.Flags().StringVarP(&organizationMembersListFlags.name, "name", "n", "", "The organization name")
	viper.BindEnv("ORG")
	if h := viper.GetString("ORG"); h != "" {
		organizationMembersListFlags.name = h
	}

	return cmd
}

// OrganizationMembersAddFlags includes flags to add a member
type OrganizationMembersAddFlags struct {
	name     string
	id       string
	memberID string
}

var organizationMembersAddFlags OrganizationMembersAddFlags

func organizationMembersAddF(cmd *cobra.Command, args []string) error {
	if organizationMembersAddFlags.id == "" && organizationMembersAddFlags.name == "" {
		return fmt.Errorf("must specify exactly one of id and name")
	}

	if organizationMembersAddFlags.id != "" && organizationMembersAddFlags.name != "" {
		return fmt.Errorf("must specify exactly one of id and name")
	}

	orgSvc, err := newOrganizationService()
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	filter := platform.OrganizationFilter{}
	if organizationMembersAddFlags.name != "" {
		filter.Name = &organizationMembersListFlags.name
	}

	if organizationMembersAddFlags.id != "" {
		var fID platform.ID
		err := fID.DecodeFromString(organizationMembersAddFlags.id)
		if err != nil {
			return fmt.Errorf("failed to decode org id %s: %v", organizationMembersAddFlags.id, err)
		}
		filter.ID = &fID
	}

	ctx := context.Background()
	organization, err := orgSvc.FindOrganization(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to find org: %v", err)
	}

	var memberID platform.ID
	err = memberID.DecodeFromString(organizationMembersAddFlags.memberID)
	if err != nil {
		return fmt.Errorf("failed to decode member id %s: %v", organizationMembersAddFlags.memberID, err)
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

	cmd.Flags().StringVarP(&organizationMembersAddFlags.id, "id", "i", "", "The organization ID")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		organizationMembersAddFlags.id = h
	}
	cmd.Flags().StringVarP(&organizationMembersAddFlags.name, "name", "n", "", "The organization name")
	viper.BindEnv("ORG")
	if h := viper.GetString("ORG"); h != "" {
		organizationMembersAddFlags.name = h
	}

	cmd.Flags().StringVarP(&organizationMembersAddFlags.memberID, "member", "o", "", "The member ID")
	cmd.MarkFlagRequired("member")

	return cmd
}

// OrganizationMembersRemoveFlags includes flags to remove a Member
type OrganizationMembersRemoveFlags struct {
	name     string
	id       string
	memberID string
}

var organizationMembersRemoveFlags OrganizationMembersRemoveFlags

func organizationMembersRemoveF(cmd *cobra.Command, args []string) error {
	if organizationMembersRemoveFlags.id == "" && organizationMembersRemoveFlags.name == "" {
		return fmt.Errorf("must specify exactly one of id and name")
	}

	if organizationMembersRemoveFlags.id != "" && organizationMembersRemoveFlags.name != "" {
		return fmt.Errorf("must specify exactly one of id and name")
	}

	orgSvc, err := newOrganizationService()
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	filter := platform.OrganizationFilter{}
	if organizationMembersRemoveFlags.name != "" {
		filter.Name = &organizationMembersRemoveFlags.name
	}

	if organizationMembersRemoveFlags.id != "" {
		var fID platform.ID
		err := fID.DecodeFromString(organizationMembersRemoveFlags.id)
		if err != nil {
			return fmt.Errorf("failed to decode org id %s: %v", organizationMembersRemoveFlags.id, err)
		}
		filter.ID = &fID
	}

	ctx := context.Background()
	organization, err := orgSvc.FindOrganization(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to find organization: %v", err)
	}

	var memberID platform.ID
	err = memberID.DecodeFromString(organizationMembersRemoveFlags.memberID)
	if err != nil {
		return fmt.Errorf("failed to decode member id %s: %v", organizationMembersRemoveFlags.memberID, err)
	}

	return membersRemoveF(ctx, organization.ID, memberID)
}

func orgMembersRemoveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove",
		Short: "Remove organization member",
		RunE:  wrapCheckSetup(organizationMembersRemoveF),
	}

	cmd.Flags().StringVarP(&organizationMembersRemoveFlags.id, "id", "i", "", "The organization ID")
	viper.BindEnv("ORG_ID")
	if h := viper.GetString("ORG_ID"); h != "" {
		organizationMembersAddFlags.id = h
	}
	cmd.Flags().StringVarP(&organizationMembersRemoveFlags.name, "name", "n", "", "The organization name")
	viper.BindEnv("ORG")
	if h := viper.GetString("ORG"); h != "" {
		organizationMembersRemoveFlags.name = h
	}
	cmd.Flags().StringVarP(&organizationMembersRemoveFlags.memberID, "member", "o", "", "The member ID")
	cmd.MarkFlagRequired("member")

	return cmd
}
