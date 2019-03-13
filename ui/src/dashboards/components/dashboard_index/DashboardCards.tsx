// Libraries
import React, {PureComponent} from 'react'

// Components
import DashboardCard from 'src/dashboards/components/dashboard_index/DashboardCard'

// Types
import {Dashboard} from 'src/types/v2'

interface Props {
  dashboards: Dashboard[]
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  showOwnerColumn: boolean
  onFilterChange: (searchTerm: string) => void
}

export default class DashboardCards extends PureComponent<Props> {
  public render() {
    const {
      dashboards,
      onCloneDashboard,
      onDeleteDashboard,
      onUpdateDashboard,
      showOwnerColumn,
      onFilterChange,
    } = this.props

    return dashboards.map(d => (
      <DashboardCard
        key={d.id}
        dashboard={d}
        onCloneDashboard={onCloneDashboard}
        onDeleteDashboard={onDeleteDashboard}
        onUpdateDashboard={onUpdateDashboard}
        showOwnerColumn={showOwnerColumn}
        onFilterChange={onFilterChange}
      />
    ))
  }
}
