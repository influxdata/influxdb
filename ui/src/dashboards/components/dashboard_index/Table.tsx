// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import _ from 'lodash'

// Components
import DashboardCards from 'src/dashboards/components/dashboard_index/DashboardCards'
import DashobardsTableEmpty from 'src/dashboards/components/dashboard_index/DashboardsTableEmpty'

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

interface StateProps {
  status: RemoteDataState
}

interface DispatchProps {
  getDashboards: typeof getDashboards
  onCreateDashboard: typeof createDashboard
  getLabels: typeof getLabels
}

type Props = OwnProps &
  StateProps &
  DispatchProps &
  RouteComponentProps<{orgID: string}>

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
        <DashobardsTableEmpty
          searchTerm={searchTerm}
          onCreateDashboard={onCreateDashboard}
          summonImportFromTemplateOverlay={this.summonImportFromTemplateOverlay}
          summonImportOverlay={this.summonImportOverlay}
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

  private summonImportOverlay = (): void => {
    const {
      history,
      match: {
        params: {orgID},
      },
    } = this.props
    history.push(`/orgs/${orgID}/dashboards/import`)
  }

  private summonImportFromTemplateOverlay = (): void => {
    const {
      history,
      match: {
        params: {orgID},
      },
    } = this.props
    history.push(`/orgs/${orgID}/dashboards/import/template`)
  }
}

const mstp = (state: AppState): StateProps => {
  const status = state.resources.dashboards.status

  return {
    status,
  }
}

const mdtp: DispatchProps = {
  getDashboards: getDashboards,
  onCreateDashboard: createDashboard,
  getLabels: getLabels,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter(DashboardsTable))
