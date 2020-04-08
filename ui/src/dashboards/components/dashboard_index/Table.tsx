// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

// Components
import {EmptyState} from '@influxdata/clockface'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'
import DashboardCards from 'src/dashboards/components/dashboard_index/DashboardCards'
import {createDashboard, getDashboards} from 'src/dashboards/actions/thunks'
import {getLabels} from 'src/labels/actions/thunks'

// Types
import {AppState, Dashboard, RemoteDataState} from 'src/types'
import {Sort, ComponentSize} from '@influxdata/clockface'
import {SortKey} from 'src/shared/components/resource_sort_dropdown/ResourceSortDropdown'
import {SortTypes} from 'src/shared/utils/sort'

interface OwnProps {
  searchTerm: string
  onFilterChange: (searchTerm: string) => void
  filterComponent?: JSX.Element
  dashboards: Dashboard[]
  sortDirection: Sort
  sortKey: SortKey
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

type Props = OwnProps & StateProps & DispatchProps & WithRouterProps

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
      if (searchTerm) {
        return (
          <EmptyState size={ComponentSize.Large} testID="empty-dashboards-list">
            <EmptyState.Text>
              No Dashboards match your search term
            </EmptyState.Text>
          </EmptyState>
        )
      }

      return (
        <EmptyState size={ComponentSize.Large} testID="empty-dashboards-list">
          <EmptyState.Text>
            Looks like you don't have any <b>Dashboards</b>, why not create one?
          </EmptyState.Text>
          <AddResourceDropdown
            onSelectNew={onCreateDashboard}
            onSelectImport={this.summonImportOverlay}
            onSelectTemplate={this.summonImportFromTemplateOverlay}
            resourceName="Dashboard"
            canImportFromTemplate={true}
          />
        </EmptyState>
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
      router,
      params: {orgID},
    } = this.props
    router.push(`/orgs/${orgID}/dashboards/import`)
  }

  private summonImportFromTemplateOverlay = (): void => {
    const {
      router,
      params: {orgID},
    } = this.props
    router.push(`/orgs/${orgID}/dashboards/import/template`)
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
)(withRouter<OwnProps>(DashboardsTable))
