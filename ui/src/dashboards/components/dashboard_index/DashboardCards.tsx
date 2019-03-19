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
  sortDirection: Sort
  sortKey: string
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  showOwnerColumn: boolean
  onFilterChange: (searchTerm: string) => void
}

interface StateProps {
  dashboards: Dashboard[]
}

type Props = OwnProps & StateProps

class DashboardCards extends PureComponent<Props> {
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

const mstp = (state: AppState, props: OwnProps) => {
  return {
    dashboards: getSortedResource(state.dashboards, props),
  }
}

export default connect<StateProps, {}, OwnProps>(mstp)(DashboardCards)
