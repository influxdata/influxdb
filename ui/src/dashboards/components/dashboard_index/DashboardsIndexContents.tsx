// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import Table from 'src/dashboards/components/dashboard_index/Table'
import FilterList from 'src/shared/components/Filter'

// Actions
import {retainRangesDashTimeV1 as retainRangesDashTimeV1Action} from 'src/dashboards/actions/ranges'
import {checkDashboardLimits as checkDashboardLimitsAction} from 'src/cloud/actions/limits'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Dashboard, AppState, RemoteDataState} from 'src/types'

interface OwnProps {
  onCreateDashboard: () => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onDeleteDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  onFilterChange: (searchTerm: string) => void
  searchTerm: string
  filterComponent?: () => JSX.Element
  onImportDashboard: () => void
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

@ErrorHandling
class DashboardsIndexContents extends Component<Props> {
  public async componentDidMount() {
    const {dashboards} = this.props

    const dashboardIDs = dashboards.map(d => d.id)
    this.props.retainRangesDashTimeV1(dashboardIDs)
    this.props.checkDashboardLimits()
  }

  public render() {
    const {
      onDeleteDashboard,
      onCloneDashboard,
      onCreateDashboard,
      onUpdateDashboard,
      searchTerm,
      dashboards,
      filterComponent,
      onFilterChange,
      onImportDashboard,
    } = this.props

    return (
      <FilterList<Dashboard>
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
            onDeleteDashboard={onDeleteDashboard}
            onCreateDashboard={onCreateDashboard}
            onCloneDashboard={onCloneDashboard}
            onUpdateDashboard={onUpdateDashboard}
            onFilterChange={onFilterChange}
            onImportDashboard={onImportDashboard}
          />
        )}
      </FilterList>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const {
    dashboards: {list: dashboards},
    cloud: {
      limits: {status},
    },
  } = state

  return {
    dashboards,
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
