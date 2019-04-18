// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import DashboardCard from 'src/dashboards/components/dashboard_index/DashboardCard'

// Selectors
import {getSortedResource} from 'src/shared/selectors/sort'

// Types
import {Dashboard, AppState} from 'src/types'
import {Sort} from 'src/clockface'

interface OwnProps {
  dashboards: Dashboard[]
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  onFilterChange: (searchTerm: string) => void
}

interface StateProps {
  sortedDashboards: Dashboard[]
}

export enum SortTypes {
  String = 'string',
  Date = 'date',
}

type Props = OwnProps & StateProps

class DashboardCards extends PureComponent<Props> {
  public state = {
    sortedDashboards: this.props.sortedDashboards,
  }

  componentDidUpdate(prevProps) {
    const {sortDirection, sortKey, sortedDashboards, dashboards} = this.props

    if (
      prevProps.sortDirection !== sortDirection ||
      prevProps.sortKey !== sortKey ||
      prevProps.dashboards.length !== dashboards.length
    ) {
      this.setState({sortedDashboards})
    }
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

const mstp = (state: AppState, props: OwnProps) => {
  return {
    sortedDashboards: getSortedResource(state.dashboards.list, props),
  }
}

export default connect<StateProps, {}, OwnProps>(mstp)(DashboardCards)
