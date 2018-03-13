import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import _ from 'lodash'

import FancyScrollbar from 'shared/components/FancyScrollbar'
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
  updateAxes,
} from 'src/dashboards/actions/cellEditorOverlay'

const formatColor = color => {
  const {hex, name} = color
  return {hex, name}
}

class TableOptions extends Component {
  state = {TimeAxis: 'VERTICAL', TimeFormat: 'mm/dd/yyyy HH:mm:ss.ss'}

  handleToggleSingleStatType = () => {}

  handleAddThreshold = () => {}

  handleDeleteThreshold = () => () => {}

  handleChooseColor = () => () => {}

  handleChooseSortBy = () => {}

  handleTimeFormatChange = () => {}

  handleToggleTimeAxis = () => {}

  handleToggleTextWrapping = () => {}

  handleColumnRename = () => {}

  handleUpdateColorValue = () => {}

  handleValidateColorValue = () => {}

  render() {
    const {
      singleStatColors,
      singleStatType,
      //   axes: {y: {prefix, suffix}},
    } = this.props

    const {TimeFormat, TimeAxis} = this.state

    const disableAddThreshold = singleStatColors.length > MAX_THRESHOLDS

    const sortedColors = _.sortBy(singleStatColors, color => color.value)

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
              TimeFormat={TimeFormat}
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
              singleStatColors={singleStatType}
            />
          </div>
        </div>
      </FancyScrollbar>
    )
  }
}

const {arrayOf, func, number, shape, string} = PropTypes

TableOptions.defaultProps = {
  colors: [],
}

TableOptions.propTypes = {
  singleStatType: string.isRequired,
  singleStatColors: arrayOf(
    shape({
      type: string.isRequired,
      hex: string.isRequired,
      id: string.isRequired,
      name: string.isRequired,
      value: number.isRequired,
    }).isRequired
  ),
  handleUpdateSingleStatType: func.isRequired,
  handleUpdateSingleStatColors: func.isRequired,
  handleUpdateAxes: func.isRequired,
  axes: shape({}).isRequired,
}

const mapStateToProps = ({
  cellEditorOverlay: {singleStatType, singleStatColors, cell: {axes}},
}) => ({
  singleStatType,
  singleStatColors,
  axes,
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
  handleUpdateAxes: bindActionCreators(updateAxes, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(TableOptions)
