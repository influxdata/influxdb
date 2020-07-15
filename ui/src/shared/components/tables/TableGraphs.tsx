// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import TableGraph from 'src/shared/components/tables/TableGraph'
import TableSidebar from 'src/shared/components/tables/TableSidebar'
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'

// Actions
import {setFieldOptions as setFieldOptionsAction} from 'src/timeMachine/actions'

// Utils
import {getDeep} from 'src/utils/wrappers'

// Types
import {TableViewProperties, FluxTable, TimeZone, Theme} from 'src/types'

interface PassedProps {
  tables: FluxTable[]
  properties: TableViewProperties
  timeZone: TimeZone
  theme: Theme
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = PassedProps & ReduxProps

interface State {
  selectedTableName: string
}

@ErrorHandling
class TableGraphs extends PureComponent<Props, State> {
  public state = {
    selectedTableName: getDeep<string>(this, 'props.tables[0].name', null),
  }

  public render() {
    const {tables, properties, timeZone, theme} = this.props

    return (
      <div className="time-machine-tables">
        {this.showSidebar && (
          <TableSidebar
            data={tables}
            selectedTableName={this.nameOfSelectedTable}
            onSelectTable={this.handleSelectTable}
            theme={theme}
          />
        )}
        {this.shouldShowTable && (
          <TableGraph
            key={this.nameOfSelectedTable}
            table={this.selectedTable}
            properties={properties}
            timeZone={timeZone}
            theme={theme}
          />
        )}
        {!this.hasData && (
          <EmptyGraphMessage message="This table has no data" />
        )}
      </div>
    )
  }

  private get nameOfSelectedTable(): string {
    const {tables} = this.props

    const isNameInTables = tables.find(
      t => t.name === this.state.selectedTableName
    )

    if (!isNameInTables) {
      return this.defaultTableName
    }

    return this.state.selectedTableName
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
    const {data} = this.selectedTable
    return !!data && !!data.length
  }

  private get shouldShowTable(): boolean {
    return !!this.props.tables && !!this.selectedTable
  }

  private get selectedTable(): FluxTable {
    const {tables} = this.props
    return tables.find(t => t.name === this.nameOfSelectedTable)
  }
}

const mdtp = {
  setFieldOptions: setFieldOptionsAction,
}

const connector = connect(null, mdtp)

export default connector(TableGraphs)
