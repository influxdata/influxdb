// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

// Components
import {EmptyState, ResourceList} from '@influxdata/clockface'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'
import DashboardCards from 'src/dashboards/components/dashboard_index/DashboardCards'

// Types
import {Dashboard} from 'src/types'
import {Sort, ComponentSize} from '@influxdata/clockface'
import {SortTypes} from 'src/shared/utils/sort'

interface OwnProps {
  searchTerm: string
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCreateDashboard: () => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  onFilterChange: (searchTerm: string) => void
  filterComponent?: JSX.Element
  onImportDashboard: () => void
  dashboards: Dashboard[]
}

interface State {
  sortKey: SortKey
  sortDirection: Sort
  sortType: SortTypes
}

type SortKey = keyof Dashboard | 'modified'

type Props = OwnProps & WithRouterProps

class DashboardsTable extends PureComponent<Props, State> {
  state: State = {
    sortKey: 'name',
    sortDirection: Sort.Ascending,
    sortType: SortTypes.String,
  }

  public render() {
    const {
      dashboards,
      filterComponent,
      onCloneDashboard,
      onDeleteDashboard,
      onUpdateDashboard,
      onFilterChange,
    } = this.props

    const {sortKey, sortDirection, sortType} = this.state

    return (
      <ResourceList>
        <ResourceList.Header filterComponent={filterComponent}>
          <ResourceList.Sorter
            name={this.headerKeys[0]}
            sortKey={this.headerKeys[0]}
            sort={sortKey === this.headerKeys[0] ? sortDirection : Sort.None}
            onClick={this.handleClickColumn}
          />
          <ResourceList.Sorter
            name={this.headerKeys[1]}
            sortKey={this.headerKeys[1]}
            sort={sortKey === this.headerKeys[1] ? sortDirection : Sort.None}
            onClick={this.handleClickColumn}
          />
        </ResourceList.Header>
        <ResourceList.Body emptyState={this.emptyState}>
          {!!dashboards.length && (
            <DashboardCards
              dashboards={dashboards}
              sortKey={sortKey}
              sortDirection={sortDirection}
              sortType={sortType}
              onCloneDashboard={onCloneDashboard}
              onDeleteDashboard={onDeleteDashboard}
              onUpdateDashboard={onUpdateDashboard}
              onFilterChange={onFilterChange}
            />
          )}
        </ResourceList.Body>
      </ResourceList>
    )
  }

  private get headerKeys(): SortKey[] {
    return ['name', 'modified']
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    let sortType = SortTypes.String

    if (sortKey === 'modified') {
      sortType = SortTypes.Date
    }

    this.setState({sortKey, sortDirection: nextSort, sortType})
  }

  private summonImportFromTemplateOverlay = (): void => {
    const {
      router,
      params: {orgID},
    } = this.props
    router.push(`/orgs/${orgID}/dashboards/import/template`)
  }

  private get emptyState(): JSX.Element {
    const {onCreateDashboard, searchTerm, onImportDashboard} = this.props

    if (searchTerm) {
      return (
        <EmptyState size={ComponentSize.Large} testID="empty-dashboards-list">
          <EmptyState.Text text="No Dashboards match your search term" />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Large} testID="empty-dashboards-list">
        <EmptyState.Text
          text="Looks like you don’t have any Dashboards , why not create one?"
          highlightWords={['Dashboards']}
        />
        <AddResourceDropdown
          onSelectNew={onCreateDashboard}
          onSelectImport={onImportDashboard}
          onSelectTemplate={this.summonImportFromTemplateOverlay}
          resourceName="Dashboard"
          canImportFromTemplate={true}
        />
      </EmptyState>
    )
  }
}

export default withRouter<OwnProps>(DashboardsTable)
