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
import {Dashboard, Organization} from 'src/types/v2'

interface Props {
  searchTerm: string
  dashboards: Dashboard[]
  defaultDashboardLink: string
  onDeleteDashboard: (dashboard: Dashboard) => void
  onCreateDashboard: () => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onExportDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  onSetDefaultDashboard: (dashboardLink: string) => void
  onEditLabels: (dashboard: Dashboard) => void
  orgs: Organization[]
  showInlineEdit?: boolean
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
    const {sortKey, sortDirection} = this.state
    const headerKeys: SortKey[] = ['name', 'owner', 'modified', 'default']

    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell
            columnName={headerKeys[0]}
            sortKey={headerKeys[0]}
            sort={sortKey === headerKeys[0] ? sortDirection : Sort.None}
            width="62%"
            onClick={this.handleClickColumn}
          />
          <IndexList.HeaderCell
            columnName={headerKeys[1]}
            sortKey={headerKeys[1]}
            sort={sortKey === headerKeys[1] ? sortDirection : Sort.None}
            width="17%"
            onClick={this.handleClickColumn}
          />
          <IndexList.HeaderCell
            columnName={headerKeys[2]}
            sortKey={headerKeys[2]}
            sort={sortKey === headerKeys[2] ? sortDirection : Sort.None}
            width="11%"
            onClick={this.handleClickColumn}
          />
          <IndexList.HeaderCell
            columnName=""
            width="10%"
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
      onExportDashboard,
      onCloneDashboard,
      onDeleteDashboard,
      onUpdateDashboard,
      onEditLabels,
      orgs,
      showInlineEdit,
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
            <TableRows
              dashboards={ds}
              onCloneDashboard={onCloneDashboard}
              onExportDashboard={onExportDashboard}
              onDeleteDashboard={onDeleteDashboard}
              onUpdateDashboard={onUpdateDashboard}
              onEditLabels={onEditLabels}
              orgs={orgs}
              showInlineEdit={showInlineEdit}
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
    const {onCreateDashboard, searchTerm} = this.props

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
        <Button
          text="Create a Dashboard"
          icon={IconFont.Plus}
          color={ComponentColor.Primary}
          onClick={onCreateDashboard}
          size={ComponentSize.Medium}
        />
      </EmptyState>
    )
  }
}

export default withRouter<Props>(DashboardsTable)
