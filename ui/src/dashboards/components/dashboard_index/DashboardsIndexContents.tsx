// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import Table from 'src/dashboards/components/dashboard_index/Table'
import FilterList from 'src/shared/components/FilterList'

// Actions
import {retainRangesDashTimeV1 as retainRangesDashTimeV1Action} from 'src/dashboards/actions/ranges'
import {checkDashboardLimits as checkDashboardLimitsAction} from 'src/cloud/actions/limits'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Dashboard, AppState, RemoteDataState, ResourceType} from 'src/types'
import {Sort} from '@influxdata/clockface'
import {getAll} from 'src/resources/selectors'
import {SortTypes} from 'src/shared/utils/sort'
import {SortKey} from 'src/shared/components/resource_sort_dropdown/ResourceSortDropdown'

interface OwnProps {
  onFilterChange: (searchTerm: string) => void
  searchTerm: string
  filterComponent?: JSX.Element
  sortDirection: Sort
  sortType: SortTypes
  sortKey: SortKey
}

interface DispatchProps {
  retainRangesDashTimeV1: typeof retainRangesDashTimeV1Action
  checkDashboardLimits: typeof checkDashboardLimitsAction
}

interface StateProps {
  dashboards: Dashboard[]
  limitStatus: RemoteDataState
}

type Props = DispatchProps & StateProps & OwnProps

const FilterDashboards = FilterList<Dashboard>()

@ErrorHandling
class DashboardsIndexContents extends Component<Props> {
  public componentDidMount() {
    const {dashboards} = this.props

    const dashboardIDs = dashboards.map(d => d.id)
    this.props.retainRangesDashTimeV1(dashboardIDs)
    this.props.checkDashboardLimits()
  }

  public render() {
    const {
      searchTerm,
      dashboards,
      filterComponent,
      onFilterChange,
      sortDirection,
      sortType,
      sortKey,
    } = this.props

    return (
      <FilterDashboards
        list={dashboards}
        searchTerm={searchTerm}
        searchKeys={['name', 'labels[].name']}
        sortByKey="name"
      >
        {filteredDashboards => (
          <Table
            searchTerm={searchTerm}
            filterComponent={filterComponent}
            dashboards={filteredDashboards}
            onFilterChange={onFilterChange}
            sortDirection={sortDirection}
            sortType={sortType}
            sortKey={sortKey}
          />
        )}
      </FilterDashboards>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const {
    cloud: {
      limits: {status},
    },
  } = state

  return {
    dashboards: getAll<Dashboard>(state, ResourceType.Dashboards),
    limitStatus: status,
  }
}

const mdtp: DispatchProps = {
  retainRangesDashTimeV1: retainRangesDashTimeV1Action,
  checkDashboardLimits: checkDashboardLimitsAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(DashboardsIndexContents)
