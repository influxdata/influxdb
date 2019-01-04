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
import {setFieldOptions as setFieldOptionsAction} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getDeep} from 'src/utils/wrappers'
import {filterTables} from 'src/shared/utils/filterTables'

// Types
import {TableView, FieldOption} from 'src/types/v2/dashboards'
import {FluxTable} from 'src/types'

interface PassedProps {
  tables: FluxTable[]
  properties: TableView
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
    selectedTableName: this.defaultTableName,
  }

  public componentDidMount() {
    this.updateFieldOptions()
  }

  public componentDidUpdate(prevProps: Props) {
    if (!_.isEqual(this.fieldOptions, prevProps.properties.fieldOptions)) {
      this.updateFieldOptions()
    }
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
    this.updateFieldOptions()
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
    const selectedTable = filterTables(tables).find(
      t => t.name === this.state.selectedTableName
    )

    return selectedTable
  }

  private updateFieldOptions() {
    this.props.setFieldOptions(this.fieldOptions)
  }

  private get fieldOptions(): FieldOption[] {
    const {fieldOptions} = this.props.properties

    if (fieldOptions.length === 0 || this.isDefaultFieldOptions) {
      return this.extractFieldOptions()
    }

    return fieldOptions
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

  private get isDefaultFieldOptions(): boolean {
    const {fieldOptions} = this.props.properties
    const internalName = getDeep<string>(fieldOptions, '0.internalName', '')

    return (
      fieldOptions.length === 1 &&
      (internalName === 'time' || internalName === '_time')
    )
  }
}

const mdtp: DispatchProps = {
  setFieldOptions: setFieldOptionsAction,
}

export default connect<null, DispatchProps, PassedProps>(
  null,
  mdtp
)(TableGraphs)
