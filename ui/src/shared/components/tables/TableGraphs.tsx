// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
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
import {findTableNameHeaders} from 'src/dashboards/utils/tableGraph'

// Types
import {TableView, FieldOption, FluxTable, TimeZone} from 'src/types'

interface PassedProps {
  tables: FluxTable[]
  properties: TableView
  timeZone: TimeZone
}

interface DispatchProps {
  setFieldOptions: typeof setFieldOptionsAction
}

type Props = PassedProps & DispatchProps

interface State {
  selectedTableName: string
}

@ErrorHandling
class TableGraphs extends PureComponent<Props, State> {
  public state = {
    selectedTableName: getDeep<string>(this, 'props.tables[0].name', null),
  }

  public componentDidMount() {
    this.updateFieldOptions()
  }

  public componentDidUpdate(prevProps: Props, prevState: State) {
    const prevHeaders = findTableNameHeaders(
      prevProps.tables,
      prevState.selectedTableName
    )
    const rawHeaders = findTableNameHeaders(
      this.props.tables,
      this.state.selectedTableName
    )

    if (!_.isEqual(rawHeaders, prevHeaders)) {
      this.updateFieldOptions()
    }
  }

  public render() {
    const {tables, properties, timeZone} = this.props

    return (
      <div className="time-machine-tables">
        {this.showSidebar && (
          <TableSidebar
            data={tables}
            selectedTableName={this.nameOfSelectedTable}
            onSelectTable={this.handleSelectTable}
          />
        )}
        {this.shouldShowTable && (
          <TableGraph
            key={this.nameOfSelectedTable}
            table={this.selectedTable}
            properties={properties}
            timeZone={timeZone}
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
    this.updateFieldOptions()
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

  private updateFieldOptions() {
    this.props.setFieldOptions(this.extractFieldOptions())
  }

  private extractFieldOptions(): FieldOption[] {
    return this.headers.map(h => ({
      internalName: h,
      displayName: h,
      visible: true,
    }))
  }

  private get headers(): string[] {
    return getDeep(this.selectedTable, 'data.0', [])
  }
}

const mdtp: DispatchProps = {
  setFieldOptions: setFieldOptionsAction,
}

export default connect<null, DispatchProps, PassedProps>(
  null,
  mdtp
)(TableGraphs)
