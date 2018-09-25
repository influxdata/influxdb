package main

import (
	"context"
	"fmt"
	"os"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/cmd/influx/internal"
	"github.com/influxdata/platform/http"
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

func organizationCreateF(cmd *cobra.Command, args []string) {
	orgS := &http.OrganizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	o := &platform.Organization{
		Name: organizationCreateFlags.name,
	}

	if err := orgS.CreateOrganization(context.Background(), o); err != nil {
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
	s := &http.OrganizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	filter := platform.OrganizationFilter{}
	if organizationFindFlags.name != "" {
		filter.Name = &organizationFindFlags.name
	}

	if organizationFindFlags.id != "" {
		filter.ID = &platform.ID{}
		if err := filter.ID.DecodeFromString(organizationFindFlags.id); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	orgs, _, err := s.FindOrganizations(context.Background(), filter)
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
	s := &http.OrganizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	var id platform.ID
	err := id.DecodeFromString(organizationUpdateFlags.id)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	update := platform.OrganizationUpdate{}
	if organizationUpdateFlags.name != "" {
		update.Name = &organizationUpdateFlags.name
	}

	o, err := s.UpdateOrganization(context.Background(), id, update)
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
	s := &http.OrganizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	var id platform.ID
	if err := id.DecodeFromString(organizationDeleteFlags.id); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ctx := context.TODO()
	o, err := s.FindOrganizationByID(ctx, id)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err = s.DeleteOrganization(ctx, id); err != nil {
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
	orgS := &http.OrganizationService{
		Addr:  flags.host,
		Token: flags.token,
	}

	mappingS := &http.UserResourceMappingService{
		Addr:  flags.host,
		Token: flags.token,
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
		filter.ID = &platform.ID{}
		err := filter.ID.DecodeFromString(organizationMembersListFlags.id)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	organization, err := orgS.FindOrganization(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	mappingFilter := platform.UserResourceMappingFilter{
		ResourceID: organization.ID,
		UserType:   platform.Member,
	}

	mappings, _, err := mappingS.FindUserResourceMappings(context.Background(), mappingFilter)
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

	orgS := &http.OrganizationService{
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
		filter.ID = &platform.ID{}
		err := filter.ID.DecodeFromString(organizationMembersAddFlags.id)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	organization, err := orgS.FindOrganization(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	memberID := &platform.ID{}
	err = memberID.DecodeFromString(organizationMembersAddFlags.memberId)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	mapping := &platform.UserResourceMapping{
		ResourceID: organization.ID,
		UserID:     *memberID,
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

	orgS := &http.OrganizationService{
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
		filter.ID = &platform.ID{}
		err := filter.ID.DecodeFromString(organizationMembersRemoveFlags.id)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	organization, err := orgS.FindOrganization(context.Background(), filter)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	memberID := &platform.ID{}
	err = memberID.DecodeFromString(organizationMembersRemoveFlags.memberId)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err = mappingS.DeleteUserResourceMapping(context.Background(), organization.ID, *memberID); err != nil {
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
