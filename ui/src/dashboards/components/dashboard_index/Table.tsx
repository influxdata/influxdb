// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

// Components
import {EmptyState, ResourceList} from '@influxdata/clockface'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'
import DashboardCards from 'src/dashboards/components/dashboard_index/DashboardCards'
import {createDashboard, getDashboards} from 'src/dashboards/actions/thunks'
import {getLabels} from 'src/labels/actions/thunks'

// Types
import {AppState, Dashboard, RemoteDataState} from 'src/types'
import {Sort, ComponentSize} from '@influxdata/clockface'
import {SortTypes} from 'src/shared/utils/sort'

interface OwnProps {
  searchTerm: string
  onFilterChange: (searchTerm: string) => void
  filterComponent?: JSX.Element
  dashboards: Dashboard[]
}

interface State {
  sortKey: SortKey
  sortDirection: Sort
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

type SortKey = keyof Dashboard | 'meta.updatedAt'

type Props = OwnProps & StateProps & DispatchProps & WithRouterProps

class DashboardsTable extends PureComponent<Props, State> {
  state: State = {
    sortKey: 'name',
    sortDirection: Sort.Ascending,
    sortType: SortTypes.String,
  }

  public componentDidMount() {
    this.props.getDashboards()
    this.props.getLabels()
  }

  public render() {
    const {status, dashboards, filterComponent, onFilterChange} = this.props

    const {sortKey, sortDirection, sortType} = this.state

    let body

    if (status === RemoteDataState.Done && !dashboards.length) {
      body = (
        <ResourceList.Body emptyState={null}>
          {this.emptyState}
        </ResourceList.Body>
      )
    } else {
      body = (
        <ResourceList.Body style={{height: '100%'}} emptyState={null}>
          <DashboardCards
            dashboards={dashboards}
            sortKey={sortKey}
            sortDirection={sortDirection}
            sortType={sortType}
            onFilterChange={onFilterChange}
          />
        </ResourceList.Body>
      )
    }

    return (
      <ResourceList>
        <ResourceList.Header filterComponent={filterComponent}>
          <ResourceList.Sorter
            name="name"
            sortKey="name"
            sort={sortKey === 'name' ? sortDirection : Sort.None}
            onClick={this.handleClickColumn}
          />
          <ResourceList.Sorter
            name="modified"
            sortKey="meta.updatedAt"
            sort={sortKey === 'meta.updatedAt' ? sortDirection : Sort.None}
            onClick={this.handleClickColumn}
          />
        </ResourceList.Header>
        {body}
      </ResourceList>
    )
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    let sortType = SortTypes.String
    if (sortKey === 'meta.updatedAt') {
      sortType = SortTypes.Date
    }

    this.setState({sortKey, sortDirection: nextSort, sortType})
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

  private get emptyState(): JSX.Element {
    const {onCreateDashboard, searchTerm} = this.props

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
