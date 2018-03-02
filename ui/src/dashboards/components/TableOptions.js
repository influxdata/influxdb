import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import _ from 'lodash'
import uuid from 'uuid'

import FancyScrollbar from 'shared/components/FancyScrollbar'
import GraphOptionsThresholds from 'src/dashboards/components/GraphOptionsThresholds'
import GraphOptionsThresholdColoring from 'src/dashboards/components/GraphOptionsThresholdColoring'
import GraphOptionsTextWrapping from 'src/dashboards/components/GraphOptionsTextWrapping'
import GraphOptionsCustomizeColumns from 'src/dashboards/components/GraphOptionsCustomizeColumns'

import GraphOptionsTimeFormat from 'src/dashboards/components/GraphOptionsTimeFormat'
import GraphOptionsSortBy from 'src/dashboards/components/GraphOptionsSortBy'
import GraphOptionsTimeAxis from 'src/dashboards/components/GraphOptionsTimeAxis'

import {
  GAUGE_COLORS,
  DEFAULT_VALUE_MIN,
  DEFAULT_VALUE_MAX,
  MAX_THRESHOLDS,
} from 'src/dashboards/constants/gaugeColors'

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

  handleDeleteThreshold = () => {}

  handleChooseColor = () => {}

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

    const columns = ['hey', 'yo', 'what'].map(col => ({
      text: col,
      name: col,
      newName: '',
    }))
    const tableSortByOptions = ['hey', 'yo', 'what'].map(col => ({text: col}))

    return (
      <FancyScrollbar
        className="display-options--cell y-axis-controls"
        autoHide={false}
      >
        <div className="display-options--cell-wrapper">
          <h5 className="display-options--header">Table Controls</h5>
          <div className="gauge-controls">
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
