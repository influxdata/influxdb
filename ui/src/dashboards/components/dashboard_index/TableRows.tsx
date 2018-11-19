// Libraries
import React, {PureComponent} from 'react'

// Components
import TableRow from 'src/dashboards/components/dashboard_index/TableRow'

// Types
import {Dashboard} from 'src/types/v2'

interface Props {
  dashboards: Dashboard[]
  defaultDashboardLink: string
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onExportDashboard: (dashboard: Dashboard) => void
  onSetDefaultDashboard: (dashboardLink: string) => void
}

export default class DashboardsIndexTableRows extends PureComponent<Props> {
  public render() {
    const {
      dashboards,
      onSetDefaultDashboard,
      defaultDashboardLink,
      onExportDashboard,
      onCloneDashboard,
      onDeleteDashboard,
    } = this.props

    return dashboards.map(d => (
      <TableRow
        key={d.id}
        dashboard={d}
        onSetDefaultDashboard={onSetDefaultDashboard}
        defaultDashboardLink={defaultDashboardLink}
        onExportDashboard={onExportDashboard}
        onCloneDashboard={onCloneDashboard}
        onDeleteDashboard={onDeleteDashboard}
      />
    ))
  }
}
