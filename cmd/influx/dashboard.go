package main

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influx/internal"
	"github.com/influxdata/influxdb/v2/dashboards/transport"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/spf13/cobra"
)

func cmdDashboard(f *globalFlags, opts genericCLIOpts) *cobra.Command {
	return newCmdDashboardBuilder(newDashboardSVCs, f, opts).cmdDashboards()
}

type dashboardSVCsFn func() (influxdb.DashboardService, influxdb.OrganizationService, error)

type cmdDashboardBuilder struct {
	genericCLIOpts
	*globalFlags

	svcFn dashboardSVCsFn

	ids []string
	org organization
}

func newCmdDashboardBuilder(svcFn dashboardSVCsFn, f *globalFlags, opts genericCLIOpts) *cmdDashboardBuilder {
	return &cmdDashboardBuilder{
		genericCLIOpts: opts,
		globalFlags:    f,
		svcFn:          svcFn,
	}
}

func (b *cmdDashboardBuilder) cmdDashboards() *cobra.Command {
	cmd := b.newCmd("dashboards", b.listRunE)
	cmd.Short = "List Dashboard(s)."
	cmd.Long = `
	List Dashboard(s).

	Examples:
		# list all known Dashboards
		influx dashboards

		# list all known Dashboards matching ids
		influx dashboards --id $ID1 --id $ID2

		# list all known Dashboards matching ids shorts
		influx dashboards -i $ID1 -i $ID2
`

	b.org.register(b.viper, cmd, false)
	cmd.Flags().StringArrayVarP(&b.ids, "id", "i", nil, "Dashboard ID to retrieve.")

	return cmd
}

func (b *cmdDashboardBuilder) listRunE(cmd *cobra.Command, args []string) error {
	svc, orgSVC, err := b.svcFn()
	if err != nil {
		return err
	}

	orgID, _ := b.org.getID(orgSVC)
	if orgID == 0 && len(b.ids) == 0 {
		return &influxdb.Error{
			Code: influxdb.EUnprocessableEntity,
			Msg:  "at least one of org, org-id, or id must be provided",
		}
	}

	var ids []*influxdb.ID
	for _, rawID := range b.ids {
		id, err := influxdb.IDFromString(rawID)
		if err != nil {
			return err
		}
		ids = append(ids, id)
	}

	var (
		out    []*influxdb.Dashboard
		offset int
	)
	const limit = 100
	for {
		dashboards, _, err := svc.FindDashboards(context.Background(), influxdb.DashboardFilter{
			IDs:            ids,
			OrganizationID: &orgID,
		}, influxdb.FindOptions{
			Limit:  limit,
			Offset: offset,
		})
		if err != nil && influxdb.ErrorCode(err) != influxdb.ENotFound {
			return err
		}
		out = append(out, dashboards...)
		if len(dashboards) < limit {
			break
		}
		offset += len(dashboards)
	}

	return b.writeDashboards(out...)
}

func (b *cmdDashboardBuilder) writeDashboards(dashboards ...*influxdb.Dashboard) error {
	if b.json {
		return b.writeJSON(dashboards)
	}

	tabW := b.newTabWriter()
	defer tabW.Flush()

	writeDashboardRows(tabW, dashboards...)
	return nil
}

func writeDashboardRows(tabW *internal.TabWriter, dashboards ...*influxdb.Dashboard) {
	tabW.WriteHeaders("ID", "OrgID", "Name", "Description", "Num Cells")
	for _, d := range dashboards {
		tabW.Write(map[string]interface{}{
			"ID":          d.ID,
			"OrgID":       d.OrganizationID.String(),
			"Name":        d.Name,
			"Description": d.Description,
			"Num Cells":   len(d.Cells),
		})
	}
}

func (b *cmdDashboardBuilder) newCmd(use string, runE func(*cobra.Command, []string) error) *cobra.Command {
	cmd := b.genericCLIOpts.newCmd(use, runE, true)
	b.genericCLIOpts.registerPrintOptions(cmd)
	b.globalFlags.registerFlags(b.viper, cmd)
	return cmd
}

func newDashboardSVCs() (influxdb.DashboardService, influxdb.OrganizationService, error) {
	httpClient, err := newHTTPClient()
	if err != nil {
		return nil, nil, err
	}

	orgSVC := &tenant.OrgClientService{
		Client: httpClient,
	}
	dashSVC := &transport.DashboardService{
		Client: httpClient,
	}
	return dashSVC, orgSVC, nil
}
