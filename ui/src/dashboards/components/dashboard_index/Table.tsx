// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

// Components
import {ComponentSize} from '@influxdata/clockface'
import {EmptyState, ResourceList} from 'src/clockface'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'
import DashboardCards from 'src/dashboards/components/dashboard_index/DashboardCards'
import SortingHat from 'src/shared/components/sorting_hat/SortingHat'

// Types
import {Sort} from 'src/clockface'
import {Dashboard} from 'src/types'

interface Props {
  searchTerm: string
  dashboards: Dashboard[]
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCreateDashboard: () => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  onFilterChange: (searchTerm: string) => void
  showOwnerColumn: boolean
  filterComponent?: () => JSX.Element
  onImportDashboard: () => void
}

interface DatedDashboard extends Dashboard {
  modified: Date
}

interface State {
  sortKey: SortKey
  sortDirection: Sort
}

type SortKey = keyof Dashboard | 'modified' | 'owner' | 'default' // owner and modified are currently hardcoded

class DashboardsTable extends PureComponent<Props & WithRouterProps, State> {
  constructor(props) {
    super(props)
    this.state = {
      sortKey: null,
      sortDirection: Sort.Descending,
    }
  }

  public render() {
    const {filterComponent} = this.props
    const {sortKey, sortDirection} = this.state

    return (
      <ResourceList>
        <ResourceList.Header filterComponent={filterComponent}>
          <ResourceList.Sorter
            name={this.headerKeys[0]}
            sortKey={this.headerKeys[0]}
            sort={sortKey === this.headerKeys[0] ? sortDirection : Sort.None}
            onClick={this.handleClickColumn}
          />
          {this.ownerSorter}
          <ResourceList.Sorter
            name={this.headerKeys[2]}
            sortKey={this.headerKeys[2]}
            sort={sortKey === this.headerKeys[2] ? sortDirection : Sort.None}
            onClick={this.handleClickColumn}
          />
        </ResourceList.Header>
        <ResourceList.Body emptyState={this.emptyState}>
          {this.sortedCards}
        </ResourceList.Body>
      </ResourceList>
    )
  }

  private get headerKeys(): SortKey[] {
    return ['name', 'owner', 'modified', 'default']
  }

  private get ownerSorter(): JSX.Element {
    const {showOwnerColumn} = this.props
    const {sortKey, sortDirection} = this.state

    if (showOwnerColumn) {
      return (
        <ResourceList.Sorter
          name={this.headerKeys[1]}
          sortKey={this.headerKeys[1]}
          sort={sortKey === this.headerKeys[1] ? sortDirection : Sort.None}
          onClick={this.handleClickColumn}
        />
      )
    }
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    this.setState({sortKey, sortDirection: nextSort})
  }

  private get sortedCards(): JSX.Element {
    const {
      dashboards,
      onCloneDashboard,
      onDeleteDashboard,
      onUpdateDashboard,
      showOwnerColumn,
      onFilterChange,
    } = this.props

    const {sortKey, sortDirection} = this.state

    if (dashboards.length) {
      return (
        <SortingHat<DatedDashboard>
          list={this.datedDashboards}
          sortKey={sortKey}
          direction={sortDirection}
        >
          {ds => (
            <DashboardCards
              dashboards={ds}
              onCloneDashboard={onCloneDashboard}
              onDeleteDashboard={onDeleteDashboard}
              onUpdateDashboard={onUpdateDashboard}
              showOwnerColumn={showOwnerColumn}
              onFilterChange={onFilterChange}
            />
          )}
        </SortingHat>
      )
    }

    return null
  }

  private get datedDashboards(): DatedDashboard[] {
    return this.props.dashboards.map(d => ({
      ...d,
      modified: new Date(d.meta.updatedAt),
    }))
  }

  private get emptyState(): JSX.Element {
    const {onCreateDashboard, searchTerm, onImportDashboard} = this.props

    if (searchTerm) {
      return (
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text text="No Dashboards match your search term" />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text
          text="Looks like you don’t have any Dashboards , why not create one?"
          highlightWords={['Dashboards']}
        />
        <AddResourceDropdown
          onSelectNew={onCreateDashboard}
          onSelectImport={onImportDashboard}
          resourceName="Dashboard"
        />
      </EmptyState>
    )
  }
}

export default withRouter<Props>(DashboardsTable)
