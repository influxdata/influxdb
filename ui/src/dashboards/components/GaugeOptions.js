import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import _ from 'lodash'
import uuid from 'uuid'

import FancyScrollbar from 'shared/components/FancyScrollbar'
import Threshold from 'src/dashboards/components/Threshold'

import {
  COLOR_TYPE_THRESHOLD,
  THRESHOLD_COLORS,
  MAX_THRESHOLDS,
  MIN_THRESHOLDS,
} from 'shared/constants/thresholds'

import {
  updateGaugeColors,
  updateAxes,
} from 'src/dashboards/actions/cellEditorOverlay'
import {colorsNumberSchema} from 'shared/schemas'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class GaugeOptions extends Component {
  handleAddThreshold = () => {
    const {gaugeColors, handleUpdateGaugeColors, onResetFocus} = this.props
    const sortedColors = _.sortBy(gaugeColors, color => color.value)

    if (sortedColors.length <= MAX_THRESHOLDS) {
      const randomColor = _.random(0, THRESHOLD_COLORS.length - 1)

      const maxValue = sortedColors[sortedColors.length - 1].value
      const minValue = sortedColors[0].value

      const colorsValues = _.mapValues(gaugeColors, 'value')
      let randomValue

      do {
        randomValue = _.round(_.random(minValue, maxValue, true), 2)
      } while (_.includes(colorsValues, randomValue))

      const newThreshold = {
        type: COLOR_TYPE_THRESHOLD,
        id: uuid.v4(),
        value: randomValue,
        hex: THRESHOLD_COLORS[randomColor].hex,
        name: THRESHOLD_COLORS[randomColor].name,
      }

      const updatedColors = _.sortBy(
        [...gaugeColors, newThreshold],
        color => color.value
      )

      handleUpdateGaugeColors(updatedColors)
    } else {
      onResetFocus()
    }
  }

  handleDeleteThreshold = threshold => {
    const {handleUpdateGaugeColors, onResetFocus} = this.props
    const gaugeColors = this.props.gaugeColors.filter(
      color => color.id !== threshold.id
    )
    const sortedColors = _.sortBy(gaugeColors, color => color.value)

    handleUpdateGaugeColors(sortedColors)
    onResetFocus()
  }

  handleChooseColor = threshold => {
    const {handleUpdateGaugeColors} = this.props
    const gaugeColors = this.props.gaugeColors.map(
      color =>
        color.id === threshold.id
          ? {...color, hex: threshold.hex, name: threshold.name}
          : color
    )

    handleUpdateGaugeColors(gaugeColors)
  }

  handleUpdateColorValue = (threshold, value) => {
    const {handleUpdateGaugeColors} = this.props
    const gaugeColors = this.props.gaugeColors.map(
      color => (color.id === threshold.id ? {...color, value} : color)
    )

    handleUpdateGaugeColors(gaugeColors)
  }

  handleValidateColorValue = (threshold, targetValue) => {
    const {gaugeColors} = this.props

    const thresholdValue = threshold.value
    let allowedToUpdate = false

    const sortedColors = _.sortBy(gaugeColors, color => color.value)

    const minValue = sortedColors[0].value
    const maxValue = sortedColors[sortedColors.length - 1].value

    // If lowest value, make sure it is less than the next threshold
    if (thresholdValue === minValue) {
      const nextValue = sortedColors[1].value
      allowedToUpdate = targetValue < nextValue
    }
    // If highest value, make sure it is greater than the previous threshold
    if (thresholdValue === maxValue) {
      const previousValue = sortedColors[sortedColors.length - 2].value
      allowedToUpdate = previousValue < targetValue
    }
    // If not min or max, make sure new value is greater than min, less than max, and unique
    if (thresholdValue !== minValue && thresholdValue !== maxValue) {
      const greaterThanMin = targetValue > minValue
      const lessThanMax = targetValue < maxValue

      const colorsWithoutMinOrMax = sortedColors.slice(
        1,
        sortedColors.length - 1
      )

      const isUnique = !colorsWithoutMinOrMax.some(
        color => color.value === targetValue && color.id !== threshold.id
      )

      allowedToUpdate = greaterThanMin && lessThanMax && isUnique
    }

    return allowedToUpdate
  }

  handleUpdatePrefix = e => {
    const {handleUpdateAxes, axes} = this.props
    const newAxes = {...axes, y: {...axes.y, prefix: e.target.value}}

    handleUpdateAxes(newAxes)
  }

  handleUpdateSuffix = e => {
    const {handleUpdateAxes, axes} = this.props
    const newAxes = {...axes, y: {...axes.y, suffix: e.target.value}}

    handleUpdateAxes(newAxes)
  }

  get sortedGaugeColors() {
    const {gaugeColors} = this.props
    const sortedColors = _.sortBy(gaugeColors, 'value')

    return sortedColors
  }

  render() {
    const {
      gaugeColors,
      axes: {
        y: {prefix, suffix},
      },
    } = this.props

    const disableMaxColor = gaugeColors.length > MIN_THRESHOLDS
    const disableAddThreshold = gaugeColors.length > MAX_THRESHOLDS

    return (
      <FancyScrollbar
        className="display-options--cell y-axis-controls"
        autoHide={false}
      >
        <div className="display-options--cell-wrapper">
          <h5 className="display-options--header">Gauge Controls</h5>
          <div className="thresholds-list">
            <button
              className="btn btn-sm btn-primary"
              onClick={this.handleAddThreshold}
              disabled={disableAddThreshold}
            >
              <span className="icon plus" /> Add Threshold
            </button>
            {this.sortedGaugeColors.map((color, index) => (
              <Threshold
                isMin={index === 0}
                isMax={index === gaugeColors.length - 1}
                visualizationType="gauge"
                threshold={color}
                key={uuid.v4()}
                disableMaxColor={disableMaxColor}
                onChooseColor={this.handleChooseColor}
                onValidateColorValue={this.handleValidateColorValue}
                onUpdateColorValue={this.handleUpdateColorValue}
                onDeleteThreshold={this.handleDeleteThreshold}
              />
            ))}
          </div>
          <div className="graph-options-group form-group-wrapper">
            <div className="form-group col-xs-6">
              <label>Prefix</label>
              <input
                className="form-control input-sm"
                placeholder="%, MPH, etc."
                defaultValue={prefix}
                onChange={this.handleUpdatePrefix}
                maxLength="5"
              />
            </div>
            <div className="form-group col-xs-6">
              <label>Suffix</label>
              <input
                className="form-control input-sm"
                placeholder="%, MPH, etc."
                defaultValue={suffix}
                onChange={this.handleUpdateSuffix}
                maxLength="5"
              />
            </div>
          </div>
        </div>
      </FancyScrollbar>
    )
  }
}

const {func, shape} = PropTypes

GaugeOptions.propTypes = {
  gaugeColors: colorsNumberSchema,
  handleUpdateGaugeColors: func.isRequired,
  handleUpdateAxes: func.isRequired,
  axes: shape({}).isRequired,
  onResetFocus: func.isRequired,
}

const mapStateToProps = ({
  cellEditorOverlay: {
    gaugeColors,
    cell: {axes},
  },
}) => ({
  gaugeColors,
  axes,
})

const mapDispatchToProps = dispatch => ({
  handleUpdateGaugeColors: bindActionCreators(updateGaugeColors, dispatch),
  handleUpdateAxes: bindActionCreators(updateAxes, dispatch),
})
export default connect(mapStateToProps, mapDispatchToProps)(GaugeOptions)
