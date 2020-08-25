// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import DashboardCards from 'src/dashboards/components/dashboard_index/DashboardCards'
import DashboardsTableEmpty from 'src/dashboards/components/dashboard_index/DashboardsTableEmpty'

// Utilities
import {getLabels} from 'src/labels/actions/thunks'

// Actions
import {createDashboard, getDashboards} from 'src/dashboards/actions/thunks'

// Types
import {AppState, Dashboard, RemoteDataState} from 'src/types'
import {Sort} from '@influxdata/clockface'
import {DashboardSortKey} from 'src/shared/components/resource_sort_dropdown/generateSortItems'
import {SortTypes} from 'src/shared/utils/sort'

interface OwnProps {
  searchTerm: string
  onFilterChange: (searchTerm: string) => void
  filterComponent?: JSX.Element
  dashboards: Dashboard[]
  sortDirection: Sort
  sortKey: DashboardSortKey
  sortType: SortTypes
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps

class DashboardsTable extends PureComponent<Props> {
  public componentDidMount() {
    this.props.getDashboards()
    this.props.getLabels()
  }

  public render() {
    const {
      status,
      dashboards,
      onFilterChange,
      sortKey,
      sortDirection,
      sortType,
      onCreateDashboard,
      searchTerm,
    } = this.props

    if (status === RemoteDataState.Done && !dashboards.length) {
      return (
        <DashboardsTableEmpty
          searchTerm={searchTerm}
          onCreateDashboard={onCreateDashboard}
        />
      )
    }

    return (
      <DashboardCards
        dashboards={dashboards}
        sortKey={sortKey}
        sortDirection={sortDirection}
        sortType={sortType}
        onFilterChange={onFilterChange}
      />
    )
  }
}

const mstp = (state: AppState) => {
  const status = state.resources.dashboards.status

  return {
    status,
  }
}

const mdtp = {
  getDashboards: getDashboards,
  onCreateDashboard: createDashboard as any,
  getLabels: getLabels,
}

const connector = connect(mstp, mdtp)

export default connector(DashboardsTable)
