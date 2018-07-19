import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import _ from 'lodash'
import uuid from 'uuid'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import Threshold from 'src/dashboards/components/Threshold'
import GraphOptionsDecimalPlaces from 'src/dashboards/components/GraphOptionsDecimalPlaces'

import {
  COLOR_TYPE_THRESHOLD,
  THRESHOLD_COLORS,
  MAX_THRESHOLDS,
  MIN_THRESHOLDS,
} from 'src/shared/constants/thresholds'

import {
  changeDecimalPlaces,
  updateGaugeColors,
  updateAxes,
} from 'src/dashboards/actions/cellEditorOverlay'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Axes} from 'src/types'
import {DecimalPlaces} from 'src/types/dashboards'
import {ColorNumber} from 'src/types/colors'

interface Props {
  axes: Axes
  gaugeColors: ColorNumber[]
  decimalPlaces: DecimalPlaces
  onResetFocus: () => void
  handleUpdateAxes: (a: Axes) => void
  onUpdateDecimalPlaces: (d: DecimalPlaces) => void
  handleUpdateGaugeColors: (d: ColorNumber[]) => void
}

@ErrorHandling
class GaugeOptions extends PureComponent<Props> {
  public render() {
    const {gaugeColors, axes, decimalPlaces} = this.props
    const {y} = axes

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
              disabled={this.disableAddThreshold}
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
                disableMaxColor={this.disableMaxColor}
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
                defaultValue={y.prefix}
                onChange={this.handleUpdatePrefix}
                maxLength={5}
              />
            </div>
            <div className="form-group col-xs-6">
              <label>Suffix</label>
              <input
                className="form-control input-sm"
                placeholder="%, MPH, etc."
                defaultValue={y.suffix}
                onChange={this.handleUpdateSuffix}
                maxLength={5}
              />
            </div>
            <GraphOptionsDecimalPlaces
              digits={decimalPlaces.digits}
              isEnforced={decimalPlaces.isEnforced}
              onDecimalPlacesChange={this.handleDecimalPlacesChange}
            />
          </div>
        </div>
      </FancyScrollbar>
    )
  }

  private get disableMaxColor(): boolean {
    const {gaugeColors} = this.props
    return gaugeColors.length > MIN_THRESHOLDS
  }

  private get disableAddThreshold(): boolean {
    const {gaugeColors} = this.props
    return gaugeColors.length > MAX_THRESHOLDS
  }

  private handleDecimalPlacesChange = (decimalPlaces: DecimalPlaces) => {
    const {onUpdateDecimalPlaces} = this.props
    onUpdateDecimalPlaces(decimalPlaces)
  }

  private handleAddThreshold = () => {
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

      const updatedColors: ColorNumber[] = _.sortBy<ColorNumber>(
        [...gaugeColors, newThreshold],
        color => color.value
      )

      handleUpdateGaugeColors(updatedColors)
    } else {
      onResetFocus()
    }
  }

  private handleDeleteThreshold = threshold => {
    const {handleUpdateGaugeColors, onResetFocus} = this.props
    const gaugeColors = this.props.gaugeColors.filter(
      color => color.id !== threshold.id
    )
    const sortedColors = _.sortBy(gaugeColors, color => color.value)

    handleUpdateGaugeColors(sortedColors)
    onResetFocus()
  }

  private handleChooseColor = threshold => {
    const {handleUpdateGaugeColors} = this.props
    const gaugeColors = this.props.gaugeColors.map(
      color =>
        color.id === threshold.id
          ? {...color, hex: threshold.hex, name: threshold.name}
          : color
    )

    handleUpdateGaugeColors(gaugeColors)
  }

  private handleUpdateColorValue = (threshold, value) => {
    const {handleUpdateGaugeColors} = this.props
    const gaugeColors = this.props.gaugeColors.map(
      color => (color.id === threshold.id ? {...color, value} : color)
    )

    handleUpdateGaugeColors(gaugeColors)
  }

  private handleValidateColorValue = (threshold, targetValue) => {
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

  private handleUpdatePrefix = e => {
    const {handleUpdateAxes, axes} = this.props
    const newAxes = {...axes, y: {...axes.y, prefix: e.target.value}}

    handleUpdateAxes(newAxes)
  }

  private handleUpdateSuffix = e => {
    const {handleUpdateAxes, axes} = this.props
    const newAxes = {...axes, y: {...axes.y, suffix: e.target.value}}

    handleUpdateAxes(newAxes)
  }

  get sortedGaugeColors() {
    const {gaugeColors} = this.props
    const sortedColors = _.sortBy(gaugeColors, 'value')

    return sortedColors
  }
}

const mapStateToProps = ({
  cellEditorOverlay: {
    gaugeColors,
    cell: {axes, decimalPlaces},
  },
}) => ({
  decimalPlaces,
  gaugeColors,
  axes,
})

const mapDispatchToProps = {
  handleUpdateGaugeColors: updateGaugeColors,
  handleUpdateAxes: updateAxes,
  onUpdateDecimalPlaces: changeDecimalPlaces,
}

export default connect(mapStateToProps, mapDispatchToProps)(GaugeOptions)
