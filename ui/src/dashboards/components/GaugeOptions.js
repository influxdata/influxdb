import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import _ from 'lodash'
import uuid from 'node-uuid'

import FancyScrollbar from 'shared/components/FancyScrollbar'
import Threshold from 'src/dashboards/components/Threshold'

import {
  COLOR_TYPE_THRESHOLD,
  GAUGE_COLORS,
  MAX_THRESHOLDS,
  MIN_THRESHOLDS,
} from 'src/dashboards/constants/gaugeColors'

import {updateGaugeColors} from 'src/dashboards/actions/cellEditorOverlay'

class GaugeOptions extends Component {
  handleAddThreshold = () => {
    const {gaugeColors, handleUpdateGaugeColors} = this.props
    const sortedColors = _.sortBy(gaugeColors, color => color.value)

    if (sortedColors.length <= MAX_THRESHOLDS) {
      const randomColor = _.random(0, GAUGE_COLORS.length - 1)

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
        hex: GAUGE_COLORS[randomColor].hex,
        name: GAUGE_COLORS[randomColor].name,
      }

      handleUpdateGaugeColors([...gaugeColors, newThreshold])
    }
  }

  handleDeleteThreshold = threshold => () => {
    const {handleUpdateGaugeColors} = this.props
    const gaugeColors = this.props.gaugeColors.filter(
      color => color.id !== threshold.id
    )

    handleUpdateGaugeColors(gaugeColors)
  }

  handleChooseColor = threshold => chosenColor => {
    const {handleUpdateGaugeColors} = this.props
    const gaugeColors = this.props.gaugeColors.map(
      color =>
        color.id === threshold.id
          ? {...color, hex: chosenColor.hex, name: chosenColor.name}
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
        color => color.value === targetValue
      )

      allowedToUpdate = greaterThanMin && lessThanMax && isUnique
    }

    return allowedToUpdate
  }

  render() {
    const {gaugeColors} = this.props

    const disableMaxColor = gaugeColors.length > MIN_THRESHOLDS
    const disableAddThreshold = gaugeColors.length > MAX_THRESHOLDS
    const sortedColors = _.sortBy(gaugeColors, color => color.value)

    return (
      <FancyScrollbar
        className="display-options--cell y-axis-controls"
        autoHide={false}
      >
        <div className="display-options--cell-wrapper">
          <h5 className="display-options--header">Gauge Controls</h5>
          <div className="gauge-controls">
            <button
              className="btn btn-sm btn-primary gauge-controls--add-threshold"
              onClick={this.handleAddThreshold}
              disabled={disableAddThreshold}
            >
              <span className="icon plus" /> Add Threshold
            </button>
            {sortedColors.map(color =>
              <Threshold
                isMin={color.value === sortedColors[0].value}
                isMax={
                  color.value === sortedColors[sortedColors.length - 1].value
                }
                visualizationType="gauge"
                threshold={color}
                key={color.id}
                disableMaxColor={disableMaxColor}
                onChooseColor={this.handleChooseColor}
                onValidateColorValue={this.handleValidateColorValue}
                onUpdateColorValue={this.handleUpdateColorValue}
                onDeleteThreshold={this.handleDeleteThreshold}
              />
            )}
          </div>
        </div>
      </FancyScrollbar>
    )
  }
}

const {arrayOf, func, number, shape, string} = PropTypes

GaugeOptions.propTypes = {
  gaugeColors: arrayOf(
    shape({
      type: string.isRequired,
      hex: string.isRequired,
      id: string.isRequired,
      name: string.isRequired,
      value: number.isRequired,
    }).isRequired
  ),
  handleUpdateGaugeColors: func.isRequired,
}

const mapStateToProps = ({cellEditorOverlay: {gaugeColors}}) => ({
  gaugeColors,
})

const mapDispatchToProps = dispatch => ({
  handleUpdateGaugeColors: bindActionCreators(updateGaugeColors, dispatch),
})
export default connect(mapStateToProps, mapDispatchToProps)(GaugeOptions)
