import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import _ from 'lodash'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import GraphOptionsTimeFormat from 'src/dashboards/components/GraphOptionsTimeFormat'
import GraphOptionsTimeAxis from 'src/dashboards/components/GraphOptionsTimeAxis'
import GraphOptionsSortBy from 'src/dashboards/components/GraphOptionsSortBy'
import GraphOptionsTextWrapping from 'src/dashboards/components/GraphOptionsTextWrapping'
import GraphOptionsCustomizeColumns from 'src/dashboards/components/GraphOptionsCustomizeColumns'

import ThresholdsList from 'src/shared/components/ThresholdsList'
import ThresholdsListTypeToggle from 'src/shared/components/ThresholdsListTypeToggle'

import {TIME_COLUMN_DEFAULT} from 'src/shared/constants/tableGraph'
import {updateTableOptions} from 'src/dashboards/actions/cellEditorOverlay'

type TableColumn = {
  internalName: string
  displayName: string
}

type Options = {
  timeFormat: string
  verticalTimeAxis: boolean
  sortBy: TableColumn
  wrapping: string
  columnNames: TableColumn[]
}

type QueryConfig = {
  measurement: string
  fields: [
    {
      alias: string
      value: string
    }
  ]
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

  componentWillMount() {
    const {queryConfigs, handleUpdateTableOptions, tableOptions} = this.props
    const {columnNames} = tableOptions
    const timeColumn = (columnNames && columnNames.find(c => c.internalName === 'time')) || TIME_COLUMN_DEFAULT

    const columns = [
      timeColumn,
      ..._.flatten(
        queryConfigs.map(qc => {
          const {measurement, fields} = qc
          return fields.map(f => {
            const internalName = `${measurement}.${f.alias}`
            const existing = columnNames.find(c => c.internalName === internalName)
            return existing || {internalName, displayName: ''}
          })
        })
      )
    ]

    handleUpdateTableOptions({...tableOptions, columnNames: columns})
  }

  handleChooseSortBy = () => {}

  handleTimeFormatChange = timeFormat => {
    const {tableOptions, handleUpdateTableOptions} = this.props
    handleUpdateTableOptions({...tableOptions, timeFormat})
  }

  onToggleVerticalTimeAxis = verticalTimeAxis => () => {
    const {tableOptions, handleUpdateTableOptions} = this.props
    handleUpdateTableOptions({...tableOptions, verticalTimeAxis})
  }

  handleToggleTextWrapping = () => {}

  handleColumnRename = column => {
    const {handleUpdateTableOptions, tableOptions} = this.props
    const {columnNames} = tableOptions
    const updatedColumns = columnNames.map(op => (op.internalName === column.internalName ? column : op))
    handleUpdateTableOptions({...tableOptions, columnNames: updatedColumns})
  }

  render() {
    const {tableOptions: {timeFormat, columnNames: columns, verticalTimeAxis}, onResetFocus} = this.props

    const tableSortByOptions = ['cpu.mean_usage_system', 'cpu.mean_usage_idle', 'cpu.mean_usage_user'].map(col => ({
      text: col
    }))

    return (
      <FancyScrollbar className="display-options--cell y-axis-controls" autoHide={false}>
        <div className="display-options--cell-wrapper">
          <h5 className="display-options--header">Table Controls</h5>
          <div className="form-group-wrapper">
            <GraphOptionsTimeFormat timeFormat={timeFormat} onTimeFormatChange={this.handleTimeFormatChange} />
            <GraphOptionsTimeAxis
              verticalTimeAxis={verticalTimeAxis}
              onToggleVerticalTimeAxis={this.onToggleVerticalTimeAxis}
            />
            <GraphOptionsSortBy sortByOptions={tableSortByOptions} onChooseSortBy={this.handleChooseSortBy} />
            <GraphOptionsTextWrapping
              thresholdsListType="background"
              onToggleTextWrapping={this.handleToggleTextWrapping}
            />
          </div>
          <GraphOptionsCustomizeColumns columns={columns} onColumnRename={this.handleColumnRename} />
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
  tableOptions
})

const mapDispatchToProps = dispatch => ({
  handleUpdateTableOptions: bindActionCreators(updateTableOptions, dispatch)
})

export default connect(mapStateToProps, mapDispatchToProps)(TableOptions)
