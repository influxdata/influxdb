package main

import (
	"context"
	"io"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influx/internal"
	"github.com/influxdata/influxdb/v2/dbrp"
	"github.com/spf13/cobra"
)

func cmdV1DBRP(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := opt.newCmd("dbrp", nil, false)
	cmd.Aliases = []string{"dbrp"}
	cmd.Short = "Database retention policy mappings for v1 APIs"
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
	BucketID string       // Specifies the bucket ID to filter on
	DB       string       // Specifies the database to filter on
	Default  bool         // Specifies filtering on default
	ID       string       // Specifies the mapping ID to filter on
	Org      organization // required  // Specifies the organization ID to filter on
	RP       string       // Specifies the retention policy to filter on
}

func v1DBRPFindCmd(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List DBRP",
		RunE:  checkSetupRunEMiddleware(&flags)(v1DBRPFindF),
	}

	f.registerFlags(opt.viper, cmd)
	registerPrintOptions(opt.viper, cmd, &v1DBRPCRUDFlags.hideHeaders, &v1DBRPCRUDFlags.json)
	v1DBRPFindFlags.Org.register(opt.viper, cmd, false)
	cmd.Flags().StringVarP(&v1DBRPCreateFlags.BucketID, "bucket-id", "b", "", "the bucket ID to filter on")
	cmd.Flags().StringVarP(&v1DBRPCreateFlags.DB, "db", "d", "", "the v1 database to map from")
	cmd.Flags().BoolVar(&v1DBRPCreateFlags.Default, "default", false, "Specify if this mapping represents the default retention policy for the database specificed")
	cmd.Flags().StringVarP(&v1DBRPCreateFlags.RP, "rp", "r", "", "InfluxDB v1 retention policy")

	return cmd
}

func v1DBRPFindF(cmd *cobra.Command, args []string) error {
	if err := v1DBRPFindFlags.Org.validOrgFlags(&flags); err != nil {
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

	s, err := newV1DBRPService()
	if err != nil {
		return err
	}
	filter := influxdb.DBRPMappingFilterV2{OrgID: &orgID}

	if v1DBRPFindFlags.ID != "" {
		fID, err := influxdb.IDFromString(v1DBRPFindFlags.ID)
		if err != nil {
			return err
		}
		filter.ID = fID
	}

	if v1DBRPFindFlags.BucketID != "" {
		fID, err := influxdb.IDFromString(v1DBRPFindFlags.ID)
		if err != nil {
			return err
		}
		filter.BucketID = fID
	}

	if v1DBRPFindFlags.DB != "" {
		fID, err := influxdb.IDFromString(v1DBRPFindFlags.BucketID)
		if err != nil {
			return err
		}
		filter.BucketID = fID
	}

	if v1DBRPFindFlags.RP != "" {
		filter.RetentionPolicy = &v1DBRPFindFlags.RP
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
	BucketID string       // Specifies the bucket ID to filter on
	DB       string       // Specifies the database to filter on
	Default  bool         // Specifies filtering on default
	Org      organization // Specifies the organization ID to filter on
	RP       string       // Specifies the retention policy to filter on
}

func v1DBRPCreateCmd(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create DBRP",
		RunE:  checkSetupRunEMiddleware(&flags)(v1DBRPCreateF),
	}

	f.registerFlags(opt.viper, cmd)
	registerPrintOptions(opt.viper, cmd, &v1DBRPCRUDFlags.hideHeaders, &v1DBRPCRUDFlags.json)
	v1DBRPCreateFlags.Org.register(opt.viper, cmd, false)
	cmd.Flags().StringVarP(&v1DBRPCreateFlags.BucketID, "bucket-id", "b", "", "the bucket ID to filter on")
	_ = cmd.MarkFlagRequired("bucket-id")
	cmd.Flags().StringVarP(&v1DBRPCreateFlags.DB, "db", "d", "", "the v1 database to map from")
	_ = cmd.MarkFlagRequired("db")
	cmd.Flags().BoolVar(&v1DBRPCreateFlags.Default, "default", false, "Specify if this mapping represents the default retention policy for the database specificed")
	cmd.Flags().StringVarP(&v1DBRPCreateFlags.RP, "rp", "r", "", "InfluxDB v1 retention policy")
	_ = cmd.MarkFlagRequired("rp")
	return cmd
}

func v1DBRPCreateF(cmd *cobra.Command, args []string) error {
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

	dbrp := &influxdb.DBRPMappingV2{
		Database:        v1DBRPCreateFlags.DB,
		Default:         v1DBRPCreateFlags.Default,
		OrganizationID:  orgID,
		RetentionPolicy: v1DBRPCreateFlags.RP,
	}
	if err := dbrp.BucketID.DecodeFromString(v1DBRPCreateFlags.BucketID); err != nil {
		return err
	}

	s, err := newV1DBRPService()
	if err != nil {
		return err
	}
	if err := s.Create(context.Background(), dbrp); err != nil {
		return err
	}
	return v1WriteDBRPs(cmd.OutOrStdout(), v1DBRPPrintOpt{
		jsonOut:     v1DBRPCRUDFlags.json,
		hideHeaders: v1DBRPCRUDFlags.hideHeaders,
		mapping:     dbrp,
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
	ID  string       // Specifies the mapping ID to filter on
	Org organization // required  // Specifies the organization ID to filter on
}

func v1DBRPDeleteCmd(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete DBRP",
		RunE:  checkSetupRunEMiddleware(&flags)(v1DBRPDeleteF),
	}

	f.registerFlags(opt.viper, cmd)
	registerPrintOptions(opt.viper, cmd, &v1DBRPCRUDFlags.hideHeaders, &v1DBRPCRUDFlags.json)
	v1DBRPCreateFlags.Org.register(opt.viper, cmd, false)
	cmd.Flags().StringVarP(&v1DBRPDeleteFlags.ID, "id", "i", "", "The dbrp ID (required)")
	_ = cmd.MarkFlagRequired("id")
	return cmd
}

func v1DBRPDeleteF(cmd *cobra.Command, args []string) error {
	if err := v1DBRPCreateFlags.Org.validOrgFlags(&flags); err != nil {
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

	id, err := influxdb.IDFromString(v1DBRPDeleteFlags.ID)
	if err != nil {
		return err
	}

	dbrpToDelete, err := s.FindByID(context.Background(), orgID, *id)
	if err != nil {
		return err
	}

	if err := s.Delete(context.Background(), orgID, *id); err != nil {
		return err
	}
	return v1WriteDBRPs(cmd.OutOrStdout(), v1DBRPPrintOpt{
		jsonOut:     v1DBRPCRUDFlags.json,
		hideHeaders: v1DBRPCRUDFlags.hideHeaders,
		mapping:     dbrpToDelete,
	})
}

var v1DBRPUpdateFlags struct {
	ID      string       // Specifies the mapping ID to filter on
	Org     organization // required  // Specifies the organization ID to filter on
	Default *bool        // pointer nil means that Default is unset in the Flags
	RP      string       // InfluxDB v1 retention policy
}

func v1DBRPUpdateCmd(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "Update DBRP",
		RunE:  checkSetupRunEMiddleware(&flags)(v1DBRPUpdateF),
	}
	f.registerFlags(opt.viper, cmd)
	registerPrintOptions(opt.viper, cmd, &v1DBRPCRUDFlags.hideHeaders, &v1DBRPCRUDFlags.json)
	v1DBRPCreateFlags.Org.register(opt.viper, cmd, false)
	cmd.Flags().StringVarP(&v1DBRPUpdateFlags.ID, "id", "i", "", "The dbrp ID (required)")
	_ = cmd.MarkFlagRequired("id")
	cmd.Flags().StringVarP(&v1DBRPUpdateFlags.RP, "rp", "r", "", "The dbrp InfluxDB v1 retention policy")
	cmd.Flags().Bool("default", false, "If dbrp InfluxDB v1 retention policy is default, (only changes when explicitly set)") // note for update we only care about update flags that the user set
	return cmd
}

func v1DBRPUpdateF(cmd *cobra.Command, args []string) error {
	if err := v1DBRPCreateFlags.Org.validOrgFlags(&flags); err != nil {
		return err
	}
	if defaultFlg := cmd.Flags().Lookup("default"); defaultFlg.Changed {
		defaultBool, _ := cmd.Flags().GetBool("default")
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

	var id influxdb.ID
	if err := id.DecodeFromString(v1DBRPUpdateFlags.ID); err != nil {
		return err
	}

	dbrpUpdate := influxdb.DBRPMappingV2{
		OrganizationID: orgID,
		ID:             id,
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
