package main

import (
	"context"
	"fmt"
	"os"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/bolt"
	"github.com/influxdata/influxdb/cmd/influx/internal"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/internal/fs"
	"github.com/spf13/cobra"
)

// Organization Command
var organizationCmd = &cobra.Command{
	Use:     "org",
	Aliases: []string{"organization"},
	Short:   "Organization management commands",
	Run:     organizationF,
}

func organizationF(cmd *cobra.Command, args []string) {
	cmd.Usage()
}

// Create Command
type OrganizationCreateFlags struct {
	name string
}

var organizationCreateFlags OrganizationCreateFlags

func init() {
	organizationCreateCmd := &cobra.Command{
		Use:   "create",
		Short: "Create organization",
		RunE:  wrapCheckSetup(organizationCreateF),
	}

	organizationCreateCmd.Flags().StringVarP(&organizationCreateFlags.name, "name", "n", "", "The name of organization that will be created")
	organizationCreateCmd.MarkFlagRequired("name")

	organizationCmd.AddCommand(organizationCreateCmd)
}

func newOrganizationService(f Flags) (platform.OrganizationService, error) {
	if flags.local {
		boltFile, err := fs.BoltFile()
		if err != nil {
			return nil, err
		}
		c := bolt.NewClient()
		c.Path = boltFile
		if err := c.Open(context.Background()); err != nil {
			return nil, err
		}

		return c, nil
	}
	return &http.OrganizationService{
		Addr:  flags.host,
		Token: flags.token,
	}, nil
}

func organizationCreateF(cmd *cobra.Command, args []string) error {
	orgSvc, err := newOrganizationService(flags)
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

func init() {
	organizationFindCmd := &cobra.Command{
		Use:   "find",
		Short: "Find organizations",
		RunE:  wrapCheckSetup(organizationFindF),
	}

	organizationFindCmd.Flags().StringVarP(&organizationFindFlags.name, "name", "n", "", "The organization name")
	organizationFindCmd.Flags().StringVarP(&organizationFindFlags.id, "id", "i", "", "The organization ID")

	organizationCmd.AddCommand(organizationFindCmd)
}

func organizationFindF(cmd *cobra.Command, args []string) error {
	orgSvc, err := newOrganizationService(flags)
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

func init() {
	organizationUpdateCmd := &cobra.Command{
		Use:   "update",
		Short: "Update organization",
		RunE:  wrapCheckSetup(organizationUpdateF),
	}

	organizationUpdateCmd.Flags().StringVarP(&organizationUpdateFlags.id, "id", "i", "", "The organization ID (required)")
	organizationUpdateCmd.Flags().StringVarP(&organizationUpdateFlags.name, "name", "n", "", "The organization name")
	organizationUpdateCmd.MarkFlagRequired("id")

	organizationCmd.AddCommand(organizationUpdateCmd)
}

func organizationUpdateF(cmd *cobra.Command, args []string) error {
	orgSvc, err := newOrganizationService(flags)
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
	orgSvc, err := newOrganizationService(flags)
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

func init() {
	organizationDeleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete organization",
		RunE:  wrapCheckSetup(organizationDeleteF),
	}

	organizationDeleteCmd.Flags().StringVarP(&organizationDeleteFlags.id, "id", "i", "", "The organization ID (required)")
	organizationDeleteCmd.MarkFlagRequired("id")

	organizationCmd.AddCommand(organizationDeleteCmd)
}

// Member management
var organizationMembersCmd = &cobra.Command{
	Use:   "members",
	Short: "Organization membership commands",
	Run:   organizationF,
}

func init() {
	organizationCmd.AddCommand(organizationMembersCmd)
}

// List Members
type OrganizationMembersListFlags struct {
	name string
	id   string
}

var organizationMembersListFlags OrganizationMembersListFlags

func organizationMembersListF(cmd *cobra.Command, args []string) error {
	orgSvc, err := newOrganizationService(flags)
	if err != nil {
		return fmt.Errorf("failed to initialize org service client: %v", err)
	}

	mappingSvc, err := newUserResourceMappingService(flags)
	if err != nil {
		return fmt.Errorf("failed to initialize members service client: %v", err)
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

	mappingFilter := platform.UserResourceMappingFilter{
		ResourceID: organization.ID,
		UserType:   platform.Member,
	}

	mappings, _, err := mappingSvc.FindUserResourceMappings(context.Background(), mappingFilter)
	if err != nil {
		return fmt.Errorf("failed to find members: %v", err)
	}

	// TODO: look up each user and output their name
	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
	)
	for _, m := range mappings {
		w.Write(map[string]interface{}{
			"ID": m.UserID.String(),
		})
	}
	w.Flush()
	return nil
}

func init() {
	organizationMembersListCmd := &cobra.Command{
		Use:   "list",
		Short: "List organization members",
		RunE:  wrapCheckSetup(organizationMembersListF),
	}

	organizationMembersListCmd.Flags().StringVarP(&organizationMembersListFlags.id, "id", "i", "", "The organization ID")
	organizationMembersListCmd.Flags().StringVarP(&organizationMembersListFlags.name, "name", "n", "", "The organization name")

	organizationMembersCmd.AddCommand(organizationMembersListCmd)
}

// Add Member
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

	orgSvc := &http.OrganizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	mappingS := &http.UserResourceMappingService{
		Addr:  flags.host,
		Token: flags.token,
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

	organization, err := orgSvc.FindOrganization(context.Background(), filter)
	if err != nil {
		return fmt.Errorf("failed to find org: %v", err)
	}

	var memberID platform.ID
	err = memberID.DecodeFromString(organizationMembersAddFlags.memberID)
	if err != nil {
		return fmt.Errorf("failed to decode member id %s: %v", organizationMembersAddFlags.memberID, err)
	}

	mapping := &platform.UserResourceMapping{
		ResourceID: organization.ID,
		UserID:     memberID,
		UserType:   platform.Member,
	}

	if err = mappingS.CreateUserResourceMapping(context.Background(), mapping); err != nil {
		return fmt.Errorf("failed to add member: %v", err)
	}

	return nil
}

func init() {
	organizationMembersAddCmd := &cobra.Command{
		Use:   "add",
		Short: "Add organization member",
		RunE:  wrapCheckSetup(organizationMembersAddF),
	}

	organizationMembersAddCmd.Flags().StringVarP(&organizationMembersAddFlags.id, "id", "i", "", "The organization ID")
	organizationMembersAddCmd.Flags().StringVarP(&organizationMembersAddFlags.name, "name", "n", "", "The organization name")
	organizationMembersAddCmd.Flags().StringVarP(&organizationMembersAddFlags.memberID, "member", "o", "", "The member ID")
	organizationMembersAddCmd.MarkFlagRequired("member")

	organizationMembersCmd.AddCommand(organizationMembersAddCmd)
}

// Remove Member
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

	orgSvc := &http.OrganizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	mappingS := &http.UserResourceMappingService{
		Addr:  flags.host,
		Token: flags.token,
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

	organization, err := orgSvc.FindOrganization(context.Background(), filter)
	if err != nil {
		return fmt.Errorf("failed to find organization: %v", err)
	}

	var memberID platform.ID
	err = memberID.DecodeFromString(organizationMembersRemoveFlags.memberID)
	if err != nil {
		return fmt.Errorf("failed to decode member id %s: %v", organizationMembersRemoveFlags.memberID, err)
	}

	if err = mappingS.DeleteUserResourceMapping(context.Background(), organization.ID, memberID); err != nil {
		return fmt.Errorf("failed to remove member: %v", err)
	}

	return nil
}

func init() {
	organizationMembersRemoveCmd := &cobra.Command{
		Use:   "remove",
		Short: "Remove organization member",
		RunE:  wrapCheckSetup(organizationMembersRemoveF),
	}

	organizationMembersRemoveCmd.Flags().StringVarP(&organizationMembersRemoveFlags.id, "id", "i", "", "The organization ID")
	organizationMembersRemoveCmd.Flags().StringVarP(&organizationMembersRemoveFlags.name, "name", "n", "", "The organization name")
	organizationMembersRemoveCmd.Flags().StringVarP(&organizationMembersRemoveFlags.memberID, "member", "o", "", "The member ID")
	organizationMembersRemoveCmd.MarkFlagRequired("member")

	organizationMembersCmd.AddCommand(organizationMembersRemoveCmd)
}
