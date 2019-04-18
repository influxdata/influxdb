// Libraries
import React, {PureComponent} from 'react'

// Components
import DashboardCard from 'src/dashboards/components/dashboard_index/DashboardCard'

// Selectors
import {getSortedResources} from 'src/shared/selectors/sort'

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

export enum SortTypes {
  String = 'string',
  Date = 'date',
}

export default class DashboardCards extends PureComponent<Props> {
  public static getDerivedStateFromProps(props: Props) {
    return {
      sortedDashboards: getSortedResources(props.dashboards, props),
    }
  }
  public state = {
    sortedDashboards: this.props.dashboards,
  }

  public render() {
    const {
      onCloneDashboard,
      onDeleteDashboard,
      onUpdateDashboard,
      onFilterChange,
    } = this.props
    const {sortedDashboards} = this.state

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
