// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

// Components
import {
  Button,
  ComponentColor,
  IconFont,
  ComponentSize,
  EmptyState,
  IndexList,
  Alignment,
} from 'src/clockface'
import TableRows from 'src/dashboards/components/dashboard_index/TableRows'
import SortingHat from 'src/shared/components/sorting_hat/SortingHat'

// Types
import {Sort} from 'src/clockface'
import {Dashboard} from 'src/api'

interface Props {
  searchTerm: string
  dashboards: Dashboard[]
  defaultDashboardLink: string
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCreateDashboard: () => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onExportDashboard: (dashboard: Dashboard) => void
  onSetDefaultDashboard: (dashboardLink: string) => void
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
    const {sortKey, sortDirection} = this.state
    const headerKeys: SortKey[] = ['name', 'owner', 'modified', 'default']

    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell
            columnName={headerKeys[0]}
            sortKey={headerKeys[0]}
            sort={sortKey === headerKeys[0] ? sortDirection : Sort.None}
            width="50%"
            onClick={this.handleClickColumn}
          />
          <IndexList.HeaderCell
            columnName={headerKeys[1]}
            sortKey={headerKeys[1]}
            sort={sortKey === headerKeys[1] ? sortDirection : Sort.None}
            width="10%"
            onClick={this.handleClickColumn}
          />
          <IndexList.HeaderCell
            columnName={headerKeys[2]}
            sortKey={headerKeys[2]}
            sort={sortKey === headerKeys[2] ? sortDirection : Sort.None}
            width="10%"
            onClick={this.handleClickColumn}
          />
          <IndexList.HeaderCell
            columnName={headerKeys[3]}
            sortKey={headerKeys[3]}
            width="10%"
            alignment={Alignment.Center}
          />
          <IndexList.HeaderCell
            columnName=""
            width="20%"
            alignment={Alignment.Right}
          />
        </IndexList.Header>
        <IndexList.Body emptyState={this.emptyState} columnCount={5}>
          {this.sortedRows}
        </IndexList.Body>
      </IndexList>
    )
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    this.setState({sortKey, sortDirection: nextSort})
  }

  private get sortedRows(): JSX.Element {
    const {
      dashboards,
      onSetDefaultDashboard,
      defaultDashboardLink,
      onExportDashboard,
      onCloneDashboard,
      onDeleteDashboard,
    } = this.props
    const {sortKey, sortDirection} = this.state

    if (dashboards.length) {
      return (
        <SortingHat<Dashboard>
          list={dashboards}
          sortKey={sortKey}
          direction={sortDirection}
        >
          {ds => (
            <TableRows
              dashboards={ds}
              onCloneDashboard={onCloneDashboard}
              onExportDashboard={onExportDashboard}
              onDeleteDashboard={onDeleteDashboard}
              defaultDashboardLink={defaultDashboardLink}
              onSetDefaultDashboard={onSetDefaultDashboard}
            />
          )}
        </SortingHat>
      )
    }

    return null
  }

  private get emptyState(): JSX.Element {
    const {onCreateDashboard, searchTerm} = this.props

    if (searchTerm) {
      return (
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text text="No dashboards match your search term" />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text text="Looks like you don’t have any dashboards" />
        <Button
          text="Create a Dashboard"
          icon={IconFont.Plus}
          color={ComponentColor.Primary}
          onClick={onCreateDashboard}
        />
      </EmptyState>
    )
  }
}

export default withRouter<Props>(DashboardsTable)
