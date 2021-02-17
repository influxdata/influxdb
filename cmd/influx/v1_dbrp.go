package main

import (
	"context"
	"io"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influx/internal"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/influxdata/influxdb/v2/kit/cli"
	"github.com/spf13/cobra"
)

func cmdV1DBRP(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := opt.newCmd("dbrp", nil, false)
	cmd.Short = "Commands to manage database and retention policy mappings for v1 APIs"
	cmd.Run = seeHelp

	cmd.AddCommand(
		v1DBRPCreateCmd(f, opt),
		v1DBRPFindCmd(f, opt),
		v1DBRPDeleteCmd(f, opt),
		v1DBRPUpdateCmd(f, opt),
	)

	return cmd
}

var v1DBRPCRUDFlags struct {
	id          string
	json        bool
	hideHeaders bool
}

var v1DBRPFindFlags struct {
	BucketID influxdb.ID  // Specifies the bucket ID to filter on
	DB       string       // Specifies the database to filter on
	Default  *bool        // Specifies filtering on default
	ID       influxdb.ID  // Specifies the mapping ID to filter on
	Org      organization // required  // Specifies the organization ID to filter on
	RP       string       // Specifies the retention policy to filter on
}

func v1DBRPFindCmd(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List database and retention policy mappings",
		RunE:  checkSetupRunEMiddleware(&flags)(v1DBRPFindF),
		Args:  cobra.NoArgs,
	}

	f.registerFlags(opt.viper, cmd)
	registerPrintOptions(opt.viper, cmd, &v1DBRPCRUDFlags.hideHeaders, &v1DBRPCRUDFlags.json)
	v1DBRPFindFlags.Org.register(opt.viper, cmd, false)
	cli.IDVar(cmd.Flags(), &v1DBRPFindFlags.BucketID, "bucket-id", 0, "Limit results to the matching bucket id")
	cli.IDVar(cmd.Flags(), &v1DBRPFindFlags.ID, "id", 0, "Limit results to a single mapping")
	cmd.Flags().StringVar(&v1DBRPFindFlags.DB, "db", "", "Limit results to the matching database name")
	cmd.Flags().StringVar(&v1DBRPFindFlags.RP, "rp", "", "Limit results to the matching retention policy name")
	cmd.Flags().Bool("default", false, "Limit results to default mappings")

	return cmd
}

func v1DBRPFindF(cmd *cobra.Command, _ []string) error {
	if err := v1DBRPFindFlags.Org.validOrgFlags(&flags); err != nil {
		return err
	}
	if defaultFlg := cmd.Flags().Lookup("default"); defaultFlg.Changed {
		defaultBool, err := cmd.Flags().GetBool("default")
		if err != nil {
			return err
		}
		v1DBRPFindFlags.Default = &defaultBool
	}

	orgSvc, err := newOrganizationService()
	if err != nil {
		return err
	}

	orgID, err := v1DBRPFindFlags.Org.getID(orgSvc)
	if err != nil {
		return err
	}

	s, err := newV1DBRPService()
	if err != nil {
		return err
	}
	filter := influxdb.DBRPMappingFilterV2{OrgID: &orgID}

	if v1DBRPFindFlags.ID.Valid() {
		filter.ID = &v1DBRPFindFlags.ID
	}

	if v1DBRPFindFlags.BucketID.Valid() {
		filter.BucketID = &v1DBRPFindFlags.BucketID
	}

	if v1DBRPFindFlags.DB != "" {
		filter.Database = &v1DBRPFindFlags.DB
	}

	if v1DBRPFindFlags.RP != "" {
		filter.RetentionPolicy = &v1DBRPFindFlags.RP
	}

	if v1DBRPFindFlags.Default != nil {
		filter.Default = v1DBRPFindFlags.Default
	}
	dbrps, _, err := s.FindMany(context.Background(), filter, influxdb.FindOptions{})
	if err != nil {
		return err
	}

	return v1WriteDBRPs(cmd.OutOrStdout(), v1DBRPPrintOpt{
		jsonOut:     v1DBRPCRUDFlags.json,
		hideHeaders: v1DBRPCRUDFlags.hideHeaders,
		mappings:    dbrps,
	})
}

var v1DBRPCreateFlags struct {
	BucketID influxdb.ID  // Specifies the bucket ID to associate with the mapping
	DB       string       // Specifies the database of the database
	Default  bool         // Specifies filtering on default
	Org      organization // Specifies the organization ID to filter on
	RP       string       // Specifies the retention policy to filter on
}

func v1DBRPCreateCmd(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a database and retention policy mapping to an existing bucket",
		RunE:  checkSetupRunEMiddleware(&flags)(v1DBRPCreateF),
		Args:  cobra.NoArgs,
	}

	f.registerFlags(opt.viper, cmd)
	registerPrintOptions(opt.viper, cmd, &v1DBRPCRUDFlags.hideHeaders, &v1DBRPCRUDFlags.json)
	v1DBRPCreateFlags.Org.register(opt.viper, cmd, false)
	cli.IDVar(cmd.Flags(), &v1DBRPCreateFlags.BucketID, "bucket-id", 0, "The ID of the bucket to be mapped")
	_ = cmd.MarkFlagRequired("bucket-id")
	cmd.Flags().StringVar(&v1DBRPCreateFlags.DB, "db", "", "The name of the database")
	_ = cmd.MarkFlagRequired("db")
	cmd.Flags().BoolVar(&v1DBRPCreateFlags.Default, "default", false, "Identify this retention policy as the default for the database")
	cmd.Flags().StringVar(&v1DBRPCreateFlags.RP, "rp", "", "The name of the retention policy")
	_ = cmd.MarkFlagRequired("rp")
	return cmd
}

func v1DBRPCreateF(cmd *cobra.Command, _ []string) error {
	if err := v1DBRPCreateFlags.Org.validOrgFlags(&flags); err != nil {
		return err
	}
	orgSvc, err := newOrganizationService()
	if err != nil {
		return err
	}

	orgID, err := v1DBRPCreateFlags.Org.getID(orgSvc)
	if err != nil {
		return err
	}

	mapping := &influxdb.DBRPMappingV2{
		BucketID:        v1DBRPCreateFlags.BucketID,
		Database:        v1DBRPCreateFlags.DB,
		Default:         v1DBRPCreateFlags.Default,
		OrganizationID:  orgID,
		RetentionPolicy: v1DBRPCreateFlags.RP,
	}

	s, err := newV1DBRPService()
	if err != nil {
		return err
	}
	if err := s.Create(context.Background(), mapping); err != nil {
		return err
	}
	return v1WriteDBRPs(cmd.OutOrStdout(), v1DBRPPrintOpt{
		jsonOut:     v1DBRPCRUDFlags.json,
		hideHeaders: v1DBRPCRUDFlags.hideHeaders,
		mapping:     mapping,
	})
}

type v1DBRPPrintOpt struct {
	jsonOut     bool
	hideHeaders bool
	mapping     *influxdb.DBRPMappingV2
	mappings    []*influxdb.DBRPMappingV2
}

func v1WriteDBRPs(w io.Writer, printOpts v1DBRPPrintOpt) error {
	if printOpts.jsonOut {
		var v interface{} = printOpts.mappings
		if printOpts.mappings == nil {
			v = printOpts.mappings
		}
		return writeJSON(w, v)
	}

	tabW := internal.NewTabWriter(w)
	defer tabW.Flush()

	tabW.HideHeaders(printOpts.hideHeaders)

	headers := []string{
		"ID",
		"Database",
		"Bucket ID",
		"Retention Policy",
		"Default",
		"Organization ID",
	}

	tabW.WriteHeaders(headers...)

	if printOpts.mappings == nil {
		printOpts.mappings = append(printOpts.mappings, printOpts.mapping)
	}

	for _, t := range printOpts.mappings {
		m := map[string]interface{}{
			"ID":               t.ID.String(),
			"Database":         t.Database,
			"Retention Policy": t.RetentionPolicy,
			"Default":          t.Default,
			"Organization ID":  t.OrganizationID,
			"Bucket ID":        t.BucketID,
		}
		tabW.Write(m)
	}

	return nil
}

var v1DBRPDeleteFlags struct {
	ID  influxdb.ID
	Org organization
}

func v1DBRPDeleteCmd(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete a database and retention policy mapping",
		RunE:  checkSetupRunEMiddleware(&flags)(v1DBRPDeleteF),
		Args:  cobra.NoArgs,
	}

	f.registerFlags(opt.viper, cmd)
	registerPrintOptions(opt.viper, cmd, &v1DBRPCRUDFlags.hideHeaders, &v1DBRPCRUDFlags.json)
	v1DBRPDeleteFlags.Org.register(opt.viper, cmd, false)
	cli.IDVar(cmd.Flags(), &v1DBRPDeleteFlags.ID, "id", 0, "The ID of the mapping to delete")
	_ = cmd.MarkFlagRequired("id")
	return cmd
}

func v1DBRPDeleteF(cmd *cobra.Command, _ []string) error {
	if err := v1DBRPDeleteFlags.Org.validOrgFlags(&flags); err != nil {
		return err
	}
	orgSvc, err := newOrganizationService()
	if err != nil {
		return err
	}

	orgID, err := v1DBRPDeleteFlags.Org.getID(orgSvc)
	if err != nil {
		return err
	}

	s, err := newV1DBRPService()
	if err != nil {
		return err
	}

	dbrpToDelete, err := s.FindByID(context.Background(), orgID, v1DBRPDeleteFlags.ID)
	if err != nil {
		return err
	}

	if err := s.Delete(context.Background(), orgID, v1DBRPDeleteFlags.ID); err != nil {
		return err
	}
	return v1WriteDBRPs(cmd.OutOrStdout(), v1DBRPPrintOpt{
		jsonOut:     v1DBRPCRUDFlags.json,
		hideHeaders: v1DBRPCRUDFlags.hideHeaders,
		mapping:     dbrpToDelete,
	})
}

var v1DBRPUpdateFlags struct {
	ID      influxdb.ID  // Specifies the mapping ID to update
	Org     organization // Specifies the organization ID
	Default *bool        // A nil value means that Default is unset in the Flags
	RP      string       // Updated name of the retention policy
}

func v1DBRPUpdateCmd(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "Update a database and retention policy mapping",
		RunE:  checkSetupRunEMiddleware(&flags)(v1DBRPUpdateF),
		Args:  cobra.NoArgs,
	}
	f.registerFlags(opt.viper, cmd)
	registerPrintOptions(opt.viper, cmd, &v1DBRPCRUDFlags.hideHeaders, &v1DBRPCRUDFlags.json)
	v1DBRPUpdateFlags.Org.register(opt.viper, cmd, false)
	cli.IDVar(cmd.Flags(), &v1DBRPUpdateFlags.ID, "id", 0, "The ID of the mapping to be updated")
	_ = cmd.MarkFlagRequired("id")
	// note for update we only care about update flags that the user set
	cmd.Flags().StringVar(&v1DBRPUpdateFlags.RP, "rp", "", "The updated name of the retention policy")
	cmd.Flags().Bool("default", false, "Set this mapping's retention policy as the default for the mapping's database")
	return cmd
}

func v1DBRPUpdateF(cmd *cobra.Command, _ []string) error {
	if err := v1DBRPUpdateFlags.Org.validOrgFlags(&flags); err != nil {
		return err
	}
	if defaultFlg := cmd.Flags().Lookup("default"); defaultFlg.Changed {
		defaultBool, err := cmd.Flags().GetBool("default")
		if err != nil {
			return err
		}
		v1DBRPUpdateFlags.Default = &defaultBool
	}

	orgSvc, err := newOrganizationService()
	if err != nil {
		return err
	}

	orgID, err := v1DBRPUpdateFlags.Org.getID(orgSvc)
	if err != nil {
		return err
	}

	dbrpUpdate := influxdb.DBRPMappingV2{
		OrganizationID: orgID,
		ID:             v1DBRPUpdateFlags.ID,
	}

	s, err := newV1DBRPService()
	if err != nil {
		return err
	}

	oldDBRP, err := s.FindByID(context.Background(), orgID, dbrpUpdate.ID)
	if err != nil {
		return err
	}

	if v1DBRPUpdateFlags.RP != "" {
		oldDBRP.RetentionPolicy = v1DBRPUpdateFlags.RP
	}

	if v1DBRPUpdateFlags.Default != nil {
		oldDBRP.Default = *v1DBRPUpdateFlags.Default
	}

	if err := s.Update(context.Background(), oldDBRP); err != nil {
		return err
	}

	// we do the lookup again, because Update doesn't give us all fields
	newDBRP, err := s.FindByID(context.Background(), orgID, dbrpUpdate.ID)
	if err != nil {
		return err
	}
	return v1WriteDBRPs(cmd.OutOrStdout(), v1DBRPPrintOpt{
		jsonOut:     v1DBRPCRUDFlags.json,
		hideHeaders: v1DBRPCRUDFlags.hideHeaders,
		mapping:     newDBRP,
	})
}

func newV1DBRPService() (*dbrp.Client, error) {
	httpClient, err := newHTTPClient()
	if err != nil {
		return nil, err
	}

	return dbrp.NewClient(httpClient), nil
}
