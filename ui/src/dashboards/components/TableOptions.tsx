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
import GraphOptionsThresholds from 'src/dashboards/components/GraphOptionsThresholds'
import GraphOptionsThresholdColoring from 'src/dashboards/components/GraphOptionsThresholdColoring'
import {MAX_THRESHOLDS} from 'src/dashboards/constants/gaugeColors'
import {
  updateSingleStatType,
  updateSingleStatColors,
  updateTableOptions,
} from 'src/dashboards/actions/cellEditorOverlay'

type Color = {
  type: string
  hex: string
  id: string
  name: string
  value: number
}

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
  singleStatType: string
  singleStatColors: Color[]
  queryConfigs: QueryConfig[]
  handleUpdateSingleStatType: () => void
  handleUpdateSingleStatColors: () => void
  handleUpdateTableOptions: (options: Options) => void
  tableOptions: Options
}

interface State {
  columns: TableColumn[]
}

const formatColor = color => {
  const {hex, name} = color
  return {hex, name}
}

export class TableOptions extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      columns: [],
    }
  }

  componentWillMount() {
    const {queryConfigs} = this.props
    let columns = [{internalName: 'time', displayName: ''}]

    for (let i = 0; i < queryConfigs.length; i++) {
      const q = queryConfigs[i]
      const measurement = q.measurement
      const fields = q.fields
      for (let j = 0; j < fields.length; j++) {
        columns = [
          ...columns,
          {internalName: `${measurement}.${fields[j].alias}`, displayName: ''},
        ]
      }
    }
    this.setState({columns})
  }

  handleToggleSingleStatType = () => {}

  handleAddThreshold = () => {}

  handleDeleteThreshold = () => () => {}

  handleChooseColor = () => () => {}

  handleChooseSortBy = () => {}

  handleTimeFormatChange = timeFormat => {
    const {tableOptions, handleUpdateTableOptions} = this.props
    handleUpdateTableOptions({...tableOptions, timeFormat})
  }

  handleToggleTimeAxis = () => {}

  handleToggleTextWrapping = () => {}

  handleColumnRename = column => {
    const {columns} = this.state
    const updatedColumns = columns.map(
      op => (op.internalName === column.internalName ? column : op)
    )
    this.setState({columns: updatedColumns})
  }

  handleUpdateColorValue = () => {}

  handleValidateColorValue = () => {}

  render() {
    const {
      singleStatColors,
      singleStatType,
      tableOptions: {timeFormat},
    } = this.props

    const {columns} = this.state
    const disableAddThreshold = singleStatColors.length > MAX_THRESHOLDS
    const TimeAxis = 'vertical'
    const sortedColors = _.sortBy(singleStatColors, color => color.value)

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
              singleStatType={singleStatType}
              onToggleTextWrapping={this.handleToggleTextWrapping}
            />
          </div>
          <GraphOptionsCustomizeColumns
            columns={columns}
            onColumnRename={this.handleColumnRename}
          />
          <GraphOptionsThresholds
            onAddThreshold={this.handleAddThreshold}
            disableAddThreshold={disableAddThreshold}
            sortedColors={sortedColors}
            formatColor={formatColor}
            onChooseColor={this.handleChooseColor}
            onValidateColorValue={this.handleValidateColorValue}
            onUpdateColorValue={this.handleUpdateColorValue}
            onDeleteThreshold={this.handleDeleteThreshold}
          />
          <div className="form-group-wrapper graph-options-group">
            <GraphOptionsThresholdColoring
              onToggleSingleStatType={this.handleToggleSingleStatType}
              singleStatType={singleStatType}
            />
          </div>
        </div>
      </FancyScrollbar>
    )
  }
}

const mapStateToProps = ({
  cellEditorOverlay: {singleStatType, singleStatColors, cell: {tableOptions}},
}) => ({
  singleStatType,
  singleStatColors,
  tableOptions,
})

const mapDispatchToProps = dispatch => ({
  handleUpdateSingleStatType: bindActionCreators(
    updateSingleStatType,
    dispatch
  ),
  handleUpdateSingleStatColors: bindActionCreators(
    updateSingleStatColors,
    dispatch
  ),
  handleUpdateTableOptions: bindActionCreators(updateTableOptions, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(TableOptions)
