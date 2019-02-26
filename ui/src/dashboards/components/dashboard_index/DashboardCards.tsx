// Libraries
import React, {PureComponent} from 'react'

// Components
import DashboardCard from 'src/dashboards/components/dashboard_index/DashboardCard'

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
  showOwnerColumn: boolean
}

export default class DashboardCards extends PureComponent<Props> {
  public render() {
    const {
      dashboards,
      onExportDashboard,
      onCloneDashboard,
      onDeleteDashboard,
      onUpdateDashboard,
      onEditLabels,
      orgs,
      showOwnerColumn,
    } = this.props

    return dashboards.map(d => (
      <DashboardCard
        key={d.id}
        dashboard={d}
        onExportDashboard={onExportDashboard}
        onCloneDashboard={onCloneDashboard}
        onDeleteDashboard={onDeleteDashboard}
        onUpdateDashboard={onUpdateDashboard}
        onEditLabels={onEditLabels}
        orgs={orgs}
        showOwnerColumn={showOwnerColumn}
      />
    ))
  }
}
