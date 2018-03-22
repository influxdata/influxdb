import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import _ from 'lodash'

import GraphOptionsCustomizeColumns from 'src/dashboards/components/GraphOptionsCustomizeColumns'
import GraphOptionsFixFirstColumn from 'src/dashboards/components/GraphOptionsFixFirstColumn'
import GraphOptionsSortBy from 'src/dashboards/components/GraphOptionsSortBy'
import GraphOptionsTextWrapping from 'src/dashboards/components/GraphOptionsTextWrapping'
import GraphOptionsTimeAxis from 'src/dashboards/components/GraphOptionsTimeAxis'
import GraphOptionsTimeFormat from 'src/dashboards/components/GraphOptionsTimeFormat'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

import ThresholdsList from 'src/shared/components/ThresholdsList'
import ThresholdsListTypeToggle from 'src/shared/components/ThresholdsListTypeToggle'

import {updateTableOptions} from 'src/dashboards/actions/cellEditorOverlay'
import {TIME_COLUMN_DEFAULT} from 'src/shared/constants/tableGraph'

interface Option {
  text: string
  key: string
}

interface TableColumn {
  internalName: string
  displayName: string
}

interface Options {
  timeFormat: string
  verticalTimeAxis: boolean
  sortBy: TableColumn
  wrapping: string
  columnNames: TableColumn[]
  fixFirstColumn: boolean
}

interface QueryConfig {
  measurement: string
  fields: Array<{
    alias: string
    value: string
  }>
}

interface Props {
  queryConfigs: QueryConfig[]
  handleUpdateTableOptions: (options: Options) => void
  tableOptions: Options
  onResetFocus: () => void
}

export class TableOptions extends PureComponent<Props, {}> {
  constructor(props) {
    super(props)
  }

  get columnNames() {
    const {tableOptions: {columnNames}} = this.props
    return columnNames || []
  }

  get timeColumn() {
    return (
      this.columnNames.find(c => c.internalName === 'time') ||
      TIME_COLUMN_DEFAULT
    )
  }

  get computedColumnNames() {
    const {queryConfigs} = this.props

    const queryFields = _.flatten(
      queryConfigs.map(({measurement, fields}) => {
        return fields.map(({alias}) => {
          const internalName = `${measurement}.${alias}`
          const existing = this.columnNames.find(
            c => c.internalName === internalName
          )
          return existing || {internalName, displayName: ''}
        })
      })
    )

    return [this.timeColumn, ...queryFields]
  }

  public componentWillMount() {
    const {handleUpdateTableOptions, tableOptions} = this.props
    handleUpdateTableOptions({
      ...tableOptions,
      columnNames: this.computedColumnNames,
    })
  }

  public handleChooseSortBy = (option: Option) => {
    const {tableOptions, handleUpdateTableOptions} = this.props
    const sortBy = {displayName: option.text, internalName: option.key}

    handleUpdateTableOptions({...tableOptions, sortBy})
  }

  public handleTimeFormatChange = timeFormat => {
    const {tableOptions, handleUpdateTableOptions} = this.props
    handleUpdateTableOptions({...tableOptions, timeFormat})
  }

  public handleToggleVerticalTimeAxis = verticalTimeAxis => () => {
    const {tableOptions, handleUpdateTableOptions} = this.props
    handleUpdateTableOptions({...tableOptions, verticalTimeAxis})
  }

  public handleToggleFixFirstColumn = () => {
    const {handleUpdateTableOptions, tableOptions} = this.props
    const fixFirstColumn = !tableOptions.fixFirstColumn
    handleUpdateTableOptions({...tableOptions, fixFirstColumn})
  }

  public handleColumnRename = column => {
    const {handleUpdateTableOptions, tableOptions} = this.props
    const {columnNames} = tableOptions
    const updatedColumns = columnNames.map(
      op => (op.internalName === column.internalName ? column : op)
    )
    handleUpdateTableOptions({...tableOptions, columnNames: updatedColumns})
  }

  public handleToggleTextWrapping = () => {}

  public render() {
    const {
      tableOptions: {
        timeFormat,
        columnNames: columns,
        verticalTimeAxis,
        fixFirstColumn,
      },
      onResetFocus,
      tableOptions,
    } = this.props

    const tableSortByOptions = this.computedColumnNames.map(col => ({
      key: col.internalName,
      text: col.displayName || col.internalName,
    }))

    return (
      <FancyScrollbar
        className="display-options--cell y-axis-controls"
        autoHide={false}
      >
        <div className="display-options--cell-wrapper">
          <h5 className="display-options--header">Table Controls</h5>
          <div className="form-group-wrapper">
            <GraphOptionsTimeFormat
              timeFormat={timeFormat}
              onTimeFormatChange={this.handleTimeFormatChange}
            />
            <GraphOptionsTimeAxis
              verticalTimeAxis={verticalTimeAxis}
              onToggleVerticalTimeAxis={this.handleToggleVerticalTimeAxis}
            />
            <GraphOptionsSortBy
              selected={tableOptions.sortBy || TIME_COLUMN_DEFAULT}
              sortByOptions={tableSortByOptions}
              onChooseSortBy={this.handleChooseSortBy}
            />
            <GraphOptionsTextWrapping
              thresholdsListType="background"
              onToggleTextWrapping={this.handleToggleTextWrapping}
            />
            <GraphOptionsFixFirstColumn
              fixed={fixFirstColumn}
              onToggleFixFirstColumn={this.handleToggleFixFirstColumn}
            />
          </div>
          <GraphOptionsCustomizeColumns
            columns={columns}
            onColumnRename={this.handleColumnRename}
          />
          <ThresholdsList showListHeading={true} onResetFocus={onResetFocus} />
          <div className="form-group-wrapper graph-options-group">
            <ThresholdsListTypeToggle containerClass="form-group col-xs-6" />
          </div>
        </div>
      </FancyScrollbar>
    )
  }
}

const mapStateToProps = ({cellEditorOverlay: {cell: {tableOptions}}}) => ({
  tableOptions,
})

const mapDispatchToProps = dispatch => ({
  handleUpdateTableOptions: bindActionCreators(updateTableOptions, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(TableOptions)
