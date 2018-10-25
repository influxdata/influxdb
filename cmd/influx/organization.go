package main

import (
	"context"
	"fmt"
	"os"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/bolt"
	"github.com/influxdata/platform/cmd/influx/internal"
	"github.com/influxdata/platform/http"
	"github.com/influxdata/platform/internal/fs"
	"github.com/spf13/cobra"
)

// Organization Command
var organizationCmd = &cobra.Command{
	Use:     "org",
	Aliases: []string{"organization"},
	Short:   "Organization related commands",
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
		Run:   organizationCreateF,
	}

	organizationCreateCmd.Flags().StringVarP(&organizationCreateFlags.name, "name", "n", "", "name of organization that will be created")
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

func organizationCreateF(cmd *cobra.Command, args []string) {
	orgSvc, err := newOrganizationService(flags)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	o := &platform.Organization{
		Name: organizationCreateFlags.name,
	}

	if err := orgSvc.CreateOrganization(context.Background(), o); err != nil {
		fmt.Println(err)
		os.Exit(1)
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
		Run:   organizationFindF,
	}

	organizationFindCmd.Flags().StringVarP(&organizationFindFlags.name, "name", "n", "", "organization name")
	organizationFindCmd.Flags().StringVarP(&organizationFindFlags.id, "id", "i", "", "organization id")

	organizationCmd.AddCommand(organizationFindCmd)
}

func organizationFindF(cmd *cobra.Command, args []string) {
	orgSvc, err := newOrganizationService(flags)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	filter := platform.OrganizationFilter{}
	if organizationFindFlags.name != "" {
		filter.Name = &organizationFindFlags.name
	}

	if organizationFindFlags.id != "" {
		id, err := platform.IDFromString(organizationFindFlags.id)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		filter.ID = id
	}

	orgs, _, err := orgSvc.FindOrganizations(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
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
		Run:   organizationUpdateF,
	}

	organizationUpdateCmd.Flags().StringVarP(&organizationUpdateFlags.id, "id", "i", "", "organization ID (required)")
	organizationUpdateCmd.Flags().StringVarP(&organizationUpdateFlags.name, "name", "n", "", "organization name")
	organizationUpdateCmd.MarkFlagRequired("id")

	organizationCmd.AddCommand(organizationUpdateCmd)
}

func organizationUpdateF(cmd *cobra.Command, args []string) {
	orgSvc, err := newOrganizationService(flags)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var id platform.ID
	if err := id.DecodeFromString(organizationUpdateFlags.id); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	update := platform.OrganizationUpdate{}
	if organizationUpdateFlags.name != "" {
		update.Name = &organizationUpdateFlags.name
	}

	o, err := orgSvc.UpdateOrganization(context.Background(), id, update)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
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
}

// Delete command
type OrganizationDeleteFlags struct {
	id string
}

var organizationDeleteFlags OrganizationDeleteFlags

func organizationDeleteF(cmd *cobra.Command, args []string) {
	orgSvc, err := newOrganizationService(flags)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var id platform.ID
	if err := id.DecodeFromString(organizationDeleteFlags.id); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ctx := context.TODO()
	o, err := orgSvc.FindOrganizationByID(ctx, id)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err = orgSvc.DeleteOrganization(ctx, id); err != nil {
		fmt.Println(err)
		os.Exit(1)
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
}

func init() {
	organizationDeleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete organization",
		Run:   organizationDeleteF,
	}

	organizationDeleteCmd.Flags().StringVarP(&organizationDeleteFlags.id, "id", "i", "", "organization id (required)")
	organizationDeleteCmd.MarkFlagRequired("id")

	organizationCmd.AddCommand(organizationDeleteCmd)
}

// Member management
var organizationMembersCmd = &cobra.Command{
	Use:   "members",
	Short: "organization membership commands",
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

func organizationMembersListF(cmd *cobra.Command, args []string) {
	orgSvc, err := newOrganizationService(flags)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	mappingSvc, err := newUserResourceMappingService(flags)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if organizationMembersListFlags.id == "" && organizationMembersListFlags.name == "" {
		fmt.Println("must specify exactly one of id and name")
		cmd.Usage()
		os.Exit(1)
	}

	filter := platform.OrganizationFilter{}
	if organizationMembersListFlags.name != "" {
		filter.Name = &organizationMembersListFlags.name
	}

	if organizationMembersListFlags.id != "" {
		var fID platform.ID
		err := fID.DecodeFromString(organizationMembersListFlags.id)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		filter.ID = &fID
	}

	organization, err := orgSvc.FindOrganization(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	mappingFilter := platform.UserResourceMappingFilter{
		ResourceID: organization.ID,
		UserType:   platform.Member,
	}

	mappings, _, err := mappingSvc.FindUserResourceMappings(context.Background(), mappingFilter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
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
}

func init() {
	organizationMembersListCmd := &cobra.Command{
		Use:   "list",
		Short: "List organization members",
		Run:   organizationMembersListF,
	}

	organizationMembersListCmd.Flags().StringVarP(&organizationMembersListFlags.id, "id", "i", "", "organization id")
	organizationMembersListCmd.Flags().StringVarP(&organizationMembersListFlags.name, "name", "n", "", "organization name")

	organizationMembersCmd.AddCommand(organizationMembersListCmd)
}

// Add Member
type OrganizationMembersAddFlags struct {
	name     string
	id       string
	memberId string
}

var organizationMembersAddFlags OrganizationMembersAddFlags

func organizationMembersAddF(cmd *cobra.Command, args []string) {
	if organizationMembersAddFlags.id == "" && organizationMembersAddFlags.name == "" {
		fmt.Println("must specify exactly one of id and name")
		cmd.Usage()
		os.Exit(1)
	}

	if organizationMembersAddFlags.id != "" && organizationMembersAddFlags.name != "" {
		fmt.Println("must specify exactly one of id and name")
		cmd.Usage()
		os.Exit(1)
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
			fmt.Println(err)
			os.Exit(1)
		}
		filter.ID = &fID
	}

	organization, err := orgSvc.FindOrganization(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var memberID platform.ID
	err = memberID.DecodeFromString(organizationMembersAddFlags.memberId)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	mapping := &platform.UserResourceMapping{
		ResourceID: organization.ID,
		UserID:     memberID,
		UserType:   platform.Member,
	}

	if err = mappingS.CreateUserResourceMapping(context.Background(), mapping); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("Member added")
}

func init() {
	organizationMembersAddCmd := &cobra.Command{
		Use:   "add",
		Short: "Add organization member",
		Run:   organizationMembersAddF,
	}

	organizationMembersAddCmd.Flags().StringVarP(&organizationMembersAddFlags.id, "id", "i", "", "organization id")
	organizationMembersAddCmd.Flags().StringVarP(&organizationMembersAddFlags.name, "name", "n", "", "organization name")
	organizationMembersAddCmd.Flags().StringVarP(&organizationMembersAddFlags.memberId, "member", "o", "", "member id")
	organizationMembersAddCmd.MarkFlagRequired("member")

	organizationMembersCmd.AddCommand(organizationMembersAddCmd)
}

// Remove Member
type OrganizationMembersRemoveFlags struct {
	name     string
	id       string
	memberId string
}

var organizationMembersRemoveFlags OrganizationMembersRemoveFlags

func organizationMembersRemoveF(cmd *cobra.Command, args []string) {
	if organizationMembersRemoveFlags.id == "" && organizationMembersRemoveFlags.name == "" {
		fmt.Println("must specify exactly one of id and name")
		cmd.Usage()
		os.Exit(1)
	}

	if organizationMembersRemoveFlags.id != "" && organizationMembersRemoveFlags.name != "" {
		fmt.Println("must specify exactly one of id and name")
		cmd.Usage()
		os.Exit(1)
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
			fmt.Println(err)
			os.Exit(1)
		}
		filter.ID = &fID
	}

	organization, err := orgSvc.FindOrganization(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var memberID platform.ID
	err = memberID.DecodeFromString(organizationMembersRemoveFlags.memberId)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err = mappingS.DeleteUserResourceMapping(context.Background(), organization.ID, memberID); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println("Member removed")
}

func init() {
	organizationMembersRemoveCmd := &cobra.Command{
		Use:   "remove",
		Short: "Remove organization member",
		Run:   organizationMembersRemoveF,
	}

	organizationMembersRemoveCmd.Flags().StringVarP(&organizationMembersRemoveFlags.id, "id", "i", "", "organization id")
	organizationMembersRemoveCmd.Flags().StringVarP(&organizationMembersRemoveFlags.name, "name", "n", "", "organization name")
	organizationMembersRemoveCmd.Flags().StringVarP(&organizationMembersRemoveFlags.memberId, "member", "o", "", "member id")
	organizationMembersRemoveCmd.MarkFlagRequired("member")

	organizationMembersCmd.AddCommand(organizationMembersRemoveCmd)
}
