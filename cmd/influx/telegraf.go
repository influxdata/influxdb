package main

import (
	"context"
	"errors"
	"io/ioutil"

	"github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influx/internal"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/spf13/cobra"
)

func cmdTelegraf(f *globalFlags, opts genericCLIOpts) *cobra.Command {
	return newCmdTelegrafBuilder(newTelegrafSVCs, f, opts).cmdTelegrafs()
}

type telegrafSVCsFn func() (influxdb.TelegrafConfigStore, influxdb.OrganizationService, error)

type cmdTelegrafBuilder struct {
	genericCLIOpts
	*globalFlags

	svcFn telegrafSVCsFn

	desc string
	file string
	id   string
	ids  []string
	name string
	org  organization
}

func newCmdTelegrafBuilder(svcFn telegrafSVCsFn, f *globalFlags, opts genericCLIOpts) *cmdTelegrafBuilder {
	return &cmdTelegrafBuilder{
		genericCLIOpts: opts,
		globalFlags:    f,
		svcFn:          svcFn,
	}
}

func (b *cmdTelegrafBuilder) cmdTelegrafs() *cobra.Command {
	cmd := b.newCmd("telegrafs", b.listRunE)
	cmd.Short = "List Telegraf configuration(s). Subcommands manage Telegraf configurations."
	cmd.Long = `
	List Telegraf configuration(s). Subcommands manage Telegraf configurations.

	Examples:
		# list all known Telegraf configurations
		influx telegrafs

		# list Telegraf configuration corresponding to specific ID
		influx telegrafs --id $ID

		# list Telegraf configuration corresponding to specific ID shorts
		influx telegrafs -i $ID
`

	b.org.register(b.viper, cmd, false)
	cmd.Flags().StringVarP(&b.id, "id", "i", "", "Telegraf configuration ID to retrieve.")

	cmd.AddCommand(
		b.cmdCreate(),
		b.cmdRemove(),
		b.cmdUpdate(),
	)
	return cmd
}

func (b *cmdTelegrafBuilder) listRunE(cmd *cobra.Command, args []string) error {
	svc, orgSVC, err := b.svcFn()
	if err != nil {
		return err
	}

	orgID, _ := b.org.getID(orgSVC)
	if orgID == 0 && b.id == "" {
		return &errors2.Error{
			Code: errors2.EUnprocessableEntity,
			Msg:  "at least one of org, org-id, or id must be provided",
		}
	}

	if b.id != "" {
		id, err := platform.IDFromString(b.id)
		if err != nil {
			return err
		}

		cfg, err := svc.FindTelegrafConfigByID(context.Background(), *id)
		if err != nil {
			return err
		}

		return b.writeTelegrafConfig(cfg)
	}

	cfgs, _, err := svc.FindTelegrafConfigs(context.Background(),
		influxdb.TelegrafConfigFilter{
			OrgID: &orgID,
		})
	if err != nil {
		return err
	}
	return b.writeTelegrafConfig(cfgs...)
}

func (b *cmdTelegrafBuilder) cmdCreate() *cobra.Command {
	cmd := b.newCmd("create", b.createRunEFn)
	cmd.Short = "Create a Telegraf configuration"
	cmd.Long = `
	The telegrafs create command creates a new Telegraf configuration.

	Examples:
		# create new Telegraf configuration
		influx telegrafs create --name $CFG_NAME --description $CFG_DESC --file $PATH_TO_TELE_CFG

		# create new Telegraf configuration using shorts
		influx telegrafs create -n $CFG_NAME -d $CFG_DESC -f $PATH_TO_TELE_CFG

		# create a new Telegraf config with a config provided via STDIN
		cat $CONFIG_FILE | influx telegrafs create -n $CFG_NAME -d $CFG_DESC
`

	b.org.register(b.viper, cmd, false)
	b.registerTelegrafCfgFlags(cmd)

	return cmd
}

func (b *cmdTelegrafBuilder) createRunEFn(cmd *cobra.Command, args []string) error {
	svc, orgSVC, err := b.svcFn()
	if err != nil {
		return err
	}

	orgID, err := b.org.getID(orgSVC)
	if err != nil {
		return err
	}

	cfg, err := b.readConfig(b.file)
	if err != nil {
		return err
	}

	newTelegraf := influxdb.TelegrafConfig{
		OrgID:       orgID,
		Name:        b.name,
		Description: b.desc,
		Config:      cfg,
	}
	err = svc.CreateTelegrafConfig(context.Background(), &newTelegraf, 0)
	if err != nil {
		return err
	}

	return b.writeTelegrafConfig(&newTelegraf)
}

func (b *cmdTelegrafBuilder) cmdRemove() *cobra.Command {
	cmd := b.newCmd("rm", b.removeRunEFn)
	cmd.Aliases = []string{"remove"}
	cmd.Short = "Remove Telegraf configuration(s)"
	cmd.Long = `
	The telegrafs rm command removes Telegraf configuration(s).

	Examples:
		# remove a single Telegraf configuration
		influx telegrafs rm --id $ID

		# remove multiple Telegraf configurations
		influx telegrafs rm --id $ID1 --id $ID2

		# remove using short flags
		influx telegrafs rm -i $ID1
`

	cmd.Flags().StringArrayVarP(&b.ids, "id", "i", nil, "Telegraf configuration ID(s) to remove.")
	cmd.MarkFlagRequired("id")

	return cmd
}

func (b *cmdTelegrafBuilder) removeRunEFn(cmd *cobra.Command, args []string) error {
	svc, _, err := b.svcFn()
	if err != nil {
		return err
	}

	for _, rawID := range b.ids {
		id, err := platform.IDFromString(rawID)
		if err != nil {
			return err
		}

		err = svc.DeleteTelegrafConfig(context.Background(), *id)
		if err != nil && errors2.ErrorCode(err) != errors2.ENotFound {
			return err
		}
	}

	return nil
}

func (b *cmdTelegrafBuilder) cmdUpdate() *cobra.Command {
	cmd := b.newCmd("update", b.updateRunEFn)
	cmd.Short = "Update a Telegraf configuration"
	cmd.Long = `
	The telegrafs update command updates a Telegraf configuration to match the
	specified parameters. If a name or description is not provided, then are set
	to an empty string.

	Examples:
		# update new Telegraf configuration
		influx telegrafs update --id $ID --name $CFG_NAME --description $CFG_DESC --file $PATH_TO_TELE_CFG

		# update new Telegraf configuration using shorts
		influx telegrafs update -i $ID -n $CFG_NAME -d $CFG_DESC -f $PATH_TO_TELE_CFG

		# update a Telegraf config with a config provided via STDIN
		cat $CONFIG_FILE | influx telegrafs update -i $ID  -n $CFG_NAME -d $CFG_DESC
`

	b.org.register(b.viper, cmd, false)
	b.registerTelegrafCfgFlags(cmd)
	cmd.Flags().StringVarP(&b.id, "id", "i", "", "Telegraf configuration id to update")
	cmd.MarkFlagRequired("id")

	return cmd
}

func (b *cmdTelegrafBuilder) updateRunEFn(cmd *cobra.Command, args []string) error {
	svc, orgSVC, err := b.svcFn()
	if err != nil {
		return err
	}

	orgID, err := b.org.getID(orgSVC)
	if err != nil {
		return err
	}

	cfg, err := b.readConfig(b.file)
	if err != nil {
		return err
	}

	id, err := platform.IDFromString(b.id)
	if err != nil {
		return err
	}

	teleCfg := influxdb.TelegrafConfig{
		ID:          *id,
		OrgID:       orgID,
		Name:        b.name,
		Description: b.desc,
		Config:      cfg,
	}
	updatedCfg, err := svc.UpdateTelegrafConfig(context.Background(), *id, &teleCfg, 0)
	if err != nil {
		return err
	}

	return b.writeTelegrafConfig(updatedCfg)
}

func (b *cmdTelegrafBuilder) writeTelegrafConfig(cfgs ...*influxdb.TelegrafConfig) error {
	if b.json {
		return b.writeJSON(cfgs)
	}

	tabW := b.newTabWriter()
	defer tabW.Flush()

	writeTelegrafRows(tabW, cfgs...)
	return nil
}

func writeTelegrafRows(tabW *internal.TabWriter, cfgs ...*influxdb.TelegrafConfig) {
	tabW.WriteHeaders("ID", "OrgID", "Name", "Description")
	for _, cfg := range cfgs {
		tabW.Write(map[string]interface{}{
			"ID":          cfg.ID,
			"OrgID":       cfg.OrgID.String(),
			"Name":        cfg.Name,
			"Description": cfg.Description,
		})
	}
}

func (b *cmdTelegrafBuilder) registerTelegrafCfgFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&b.file, "file", "f", "", "Path to Telegraf configuration")
	cmd.Flags().StringVarP(&b.name, "name", "n", "", "Name of Telegraf configuration")
	cmd.Flags().StringVarP(&b.desc, "description", "d", "", "Description for Telegraf configuration")
}

func (b *cmdTelegrafBuilder) readConfig(file string) (string, error) {
	if file != "" {
		bb, err := ioutil.ReadFile(file)
		if err != nil {
			return "", err
		}

		return string(bb), nil
	}

	stdIn, err := inStdIn(b.in)
	if err != nil {
		return "", &errors2.Error{
			Code: errors2.EUnprocessableEntity,
			Err:  errors.New("a Telegraf config must be provided"),
		}
	}
	defer stdIn.Close()

	bb, err := ioutil.ReadAll(stdIn)
	if err != nil {
		return "", err
	}
	return string(bb), nil
}

func (b *cmdTelegrafBuilder) newCmd(use string, runE func(*cobra.Command, []string) error) *cobra.Command {
	cmd := b.genericCLIOpts.newCmd(use, runE, true)
	b.genericCLIOpts.registerPrintOptions(cmd)
	b.globalFlags.registerFlags(b.viper, cmd)
	return cmd
}

func newTelegrafSVCs() (influxdb.TelegrafConfigStore, influxdb.OrganizationService, error) {
	httpClient, err := newHTTPClient()
	if err != nil {
		return nil, nil, err
	}

	orgSVC := &tenant.OrgClientService{
		Client: httpClient,
	}

	return http.NewTelegrafService(httpClient), orgSVC, nil
}
