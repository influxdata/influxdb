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
import TableHeader from 'src/dashboards/components/dashboard_index/TableHeader'
import SortingHat from 'src/shared/components/sorting_hat/SortingHat'

// Types
import {Sort} from 'src/clockface'
import {IndexHeaderCellProps} from 'src/clockface/components/index_views/IndexListHeaderCell'
import {Dashboard} from 'src/types/v2'

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

type Column = IndexHeaderCellProps

interface State {
  columns: Column[]
}

class DashboardsTable extends PureComponent<Props & WithRouterProps, State> {
  constructor(props) {
    super(props)
    this.state = {
      columns: this.defaultColumns,
    }
  }

  public render() {
    const {
      dashboards,
      onSetDefaultDashboard,
      defaultDashboardLink,
      onExportDashboard,
      onCloneDashboard,
      onDeleteDashboard,
    } = this.props

    return (
      <IndexList>
        <TableHeader
          columns={this.state.columns}
          onClickColumn={this.handleClickColumn}
        />
        <IndexList.Body emptyState={this.emptyState} columnCount={5}>
          <SortingHat<Dashboard>
            list={dashboards}
            sortKeys={this.sortKeys}
            directions={this.directions}
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
        </IndexList.Body>
      </IndexList>
    )
  }

  private get sortKeys(): string[] {
    return this.state.columns.map(c => c.sortKey)
  }

  private get directions(): Sort[] {
    return this.state.columns.map(c => c.sort)
  }

  private handleClickColumn = (nextSort: Sort, sortKey: string) => {
    const columns = this.state.columns.map(h => {
      if (h.sortKey === sortKey) {
        return {...h, sort: nextSort}
      }

      return h
    })

    this.setState({columns})
  }

  private get defaultColumns(): Column[] {
    return [
      {
        columnName: 'name',
        sortKey: 'name',
        sort: Sort.Descending,
        width: '50%',
      },
      {
        columnName: 'owner',
        sortKey: 'owner',
        sort: Sort.Descending,
        width: '10%',
      },
      {
        columnName: 'modified',
        sortKey: 'modified',
        sort: Sort.Descending,
        width: '10%',
      },
      {
        columnName: 'default',
        sortKey: 'default',
        width: '10%',
      },
      {
        columnName: '',
        width: '20%',
        alignment: Alignment.Right,
      },
    ]
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
