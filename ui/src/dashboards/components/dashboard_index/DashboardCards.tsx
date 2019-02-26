// Libraries
import React, {PureComponent} from 'react'

// Components
import DashboardCard from 'src/dashboards/components/dashboard_index/DashboardCard'

// Types
import {Dashboard, Organization} from 'src/types/v2'
import {Label} from 'src/types/v2/labels'

interface Props {
  dashboards: Dashboard[]
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onExportDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  orgs: Organization[]
  showOwnerColumn: boolean
  onRemoveLabels: (resourceID: string, labels: Label[]) => void
}

export default class DashboardCards extends PureComponent<Props> {
  public render() {
    const {
      dashboards,
      onExportDashboard,
      onCloneDashboard,
      onDeleteDashboard,
      onUpdateDashboard,
      orgs,
      showOwnerColumn,
      onRemoveLabels,
    } = this.props

    return dashboards.map(d => (
      <DashboardCard
        key={d.id}
        dashboard={d}
        onExportDashboard={onExportDashboard}
        onCloneDashboard={onCloneDashboard}
        onDeleteDashboard={onDeleteDashboard}
        onUpdateDashboard={onUpdateDashboard}
        orgs={orgs}
        showOwnerColumn={showOwnerColumn}
        onRemoveLabels={onRemoveLabels}
      />
    ))
  }
}
