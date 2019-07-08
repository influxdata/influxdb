// Libraries
import React, {PureComponent} from 'react'
import memoizeOne from 'memoize-one'

// Components
import DashboardCard from 'src/dashboards/components/dashboard_index/DashboardCard'

// Selectors
import {getSortedResources, SortTypes} from 'src/shared/utils/sort'

// Types
import {Dashboard} from 'src/types'
import {Sort} from 'src/clockface'

interface Props {
  dashboards: Dashboard[]
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  onFilterChange: (searchTerm: string) => void
}

export default class DashboardCards extends PureComponent<Props> {
  private memGetSortedResources = memoizeOne<typeof getSortedResources>(
    getSortedResources
  )

  public render() {
    const {
      dashboards,
      sortDirection,
      sortKey,
      sortType,
      onCloneDashboard,
      onDeleteDashboard,
      onUpdateDashboard,
      onFilterChange,
    } = this.props
    const sortedDashboards = this.memGetSortedResources(
      dashboards,
      sortKey,
      sortDirection,
      sortType
    )

    return sortedDashboards.map(dashboard => (
      <DashboardCard
        key={dashboard.id}
        dashboard={dashboard}
        onCloneDashboard={onCloneDashboard}
        onDeleteDashboard={onDeleteDashboard}
        onUpdateDashboard={onUpdateDashboard}
        onFilterChange={onFilterChange}
      />
    ))
  }
}
