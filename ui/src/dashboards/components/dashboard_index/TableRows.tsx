// Libraries
import React, {PureComponent} from 'react'

// Components
import TableRow from 'src/dashboards/components/dashboard_index/TableRow'

// Types
import {Dashboard, Organization} from 'src/types/v2'

interface Props {
  dashboards: Dashboard[]
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onExportDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  onEditLabels: (dashboard: Dashboard) => void
  orgs: Organization[]
}

export default class DashboardsIndexTableRows extends PureComponent<Props> {
  public render() {
    const {
      dashboards,
      onExportDashboard,
      onCloneDashboard,
      onDeleteDashboard,
      onUpdateDashboard,
      onEditLabels,
      orgs,
    } = this.props

    return dashboards.map(d => (
      <TableRow
        key={d.id}
        dashboard={d}
        onExportDashboard={onExportDashboard}
        onCloneDashboard={onCloneDashboard}
        onDeleteDashboard={onDeleteDashboard}
        onUpdateDashboard={onUpdateDashboard}
        onEditLabels={onEditLabels}
        orgs={orgs}
      />
    ))
  }
}
