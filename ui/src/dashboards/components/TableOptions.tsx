import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import GraphOptionsTimeFormat from 'src/dashboards/components/GraphOptionsTimeFormat'
import GraphOptionsTimeAxis from 'src/dashboards/components/GraphOptionsTimeAxis'
import GraphOptionsSortBy from 'src/dashboards/components/GraphOptionsSortBy'
import GraphOptionsTextWrapping from 'src/dashboards/components/GraphOptionsTextWrapping'
import GraphOptionsCustomizeColumns from 'src/dashboards/components/GraphOptionsCustomizeColumns'
import ThresholdsList from 'src/shared/components/ThresholdsList'
import ThresholdsColorToggle from 'src/shared/components/ThresholdsColorToggle'

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
  handleChooseSortBy = () => {}

  handleTimeFormatChange = timeFormat => {
    const {tableOptions, handleUpdateTableOptions} = this.props
    handleUpdateTableOptions({...tableOptions, timeFormat})
  }

  handleToggleTimeAxis = () => {}

  handleToggleTextWrapping = () => {}

  handleColumnRename = () => {}

  render() {
    const {tableOptions: {timeFormat}, onResetFocus} = this.props

    const TimeAxis = 'vertical'

    const columns = [
      'cpu.mean_usage_system',
      'cpu.mean_usage_idle',
      'cpu.mean_usage_user',
    ].map(col => ({
      text: col,
      name: col,
      newName: '',
    }))
    const tableSortByOptions = [
      'cpu.mean_usage_system',
      'cpu.mean_usage_idle',
      'cpu.mean_usage_user',
    ].map(col => ({text: col}))

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
              TimeAxis={TimeAxis}
              onToggleTimeAxis={this.handleToggleTimeAxis}
            />
            <GraphOptionsSortBy
              sortByOptions={tableSortByOptions}
              onChooseSortBy={this.handleChooseSortBy}
            />
            <GraphOptionsTextWrapping
              onToggleTextWrapping={this.handleToggleTextWrapping}
            />
          </div>
          <GraphOptionsCustomizeColumns
            columns={columns}
            onColumnRename={this.handleColumnRename}
          />
          <ThresholdsList onResetFocus={onResetFocus} />
          <div className="form-group-wrapper graph-options-group">
            <ThresholdsColorToggle containerClass="form-group col-xs-6" />
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
