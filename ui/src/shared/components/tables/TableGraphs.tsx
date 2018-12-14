// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import TableGraph from 'src/shared/components/tables/TableGraph'
import TableSidebar from 'src/shared/components/tables/TableSidebar'
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'

// Types
import {TableView} from 'src/types/v2/dashboards'
import {FluxTable} from 'src/types'

interface Props {
  tables: FluxTable[]
  properties: TableView
}

interface State {
  selectedTableName: string
}

@ErrorHandling
class TableGraphs extends PureComponent<Props, State> {
  public state = {
    selectedTableName: this.defaultTableName,
  }

  public render() {
    const {tables, properties} = this.props
    return (
      <div className="time-machine-tables">
        {this.showSidebar && (
          <TableSidebar
            data={tables}
            selectedTableName={this.selectedTableName}
            onSelectTable={this.handleSelectTable}
          />
        )}
        {this.shouldShowTable && (
          <TableGraph
            key={this.selectedTableName}
            table={this.selectedTable}
            properties={properties}
          />
        )}
        {!this.hasData && (
          <EmptyGraphMessage message={'This table has no data'} />
        )}
      </div>
    )
  }

  private get selectedTableName(): string {
    return this.state.selectedTableName || this.defaultTableName
  }

  private get defaultTableName() {
    return _.get(this.props.tables, '0.name', null)
  }

  private handleSelectTable = (selectedTableName: string): void => {
    this.setState({selectedTableName})
  }

  private get showSidebar(): boolean {
    return this.props.tables.length > 1
  }

  private get hasData(): boolean {
    return !!this.selectedTable.data.length
  }

  private get shouldShowTable(): boolean {
    return !!this.props.tables && !!this.selectedTable
  }

  private get selectedTable(): FluxTable {
    const {tables} = this.props
    const selectedTable = tables.find(
      t => t.name === this.state.selectedTableName
    )
    return selectedTable
  }
}

export default TableGraphs
