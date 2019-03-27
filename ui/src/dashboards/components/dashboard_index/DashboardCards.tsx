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
  showOwnerColumn: boolean
  onFilterChange: (searchTerm: string) => void
}

interface StateProps {
  sortedIDs: string[]
}

export enum SortTypes {
  String = 'string',
  Date = 'date',
}

type Props = OwnProps & StateProps

class DashboardCards extends PureComponent<Props> {
  public state = {
    sortedIDs: this.props.sortedIDs,
  }

  componentDidUpdate(prevProps) {
    const {sortDirection, sortKey, sortedIDs, dashboards} = this.props

    if (
      prevProps.sortDirection !== sortDirection ||
      prevProps.sortKey !== sortKey ||
      prevProps.dashboards.length !== dashboards.length
    ) {
      this.setState({sortedIDs})
    }
  }

  public render() {
    const {
      dashboards,
      onCloneDashboard,
      onDeleteDashboard,
      onUpdateDashboard,
      showOwnerColumn,
      onFilterChange,
    } = this.props

    const {sortedIDs} = this.state

    return sortedIDs.map(id => {
      const dashboard = dashboards.find(d => d.id === id)
      return (
        dashboard && (
          <DashboardCard
            key={id}
            dashboard={dashboard}
            onCloneDashboard={onCloneDashboard}
            onDeleteDashboard={onDeleteDashboard}
            onUpdateDashboard={onUpdateDashboard}
            showOwnerColumn={showOwnerColumn}
            onFilterChange={onFilterChange}
          />
        )
      )
    })
  }
}

const mstp = (state: AppState, props: OwnProps) => {
  return {
    sortedIDs: getSortedResource(state.dashboards.list, props),
  }
}

export default connect<StateProps, {}, OwnProps>(mstp)(DashboardCards)
