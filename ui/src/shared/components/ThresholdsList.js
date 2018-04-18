import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import _ from 'lodash'
import uuid from 'uuid'

import Threshold from 'src/dashboards/components/Threshold'
import ColorDropdown from 'shared/components/ColorDropdown'

import {updateThresholdsListColors} from 'src/dashboards/actions/cellEditorOverlay'
import {colorsNumberSchema} from 'shared/schemas'

import {
  THRESHOLD_COLORS,
  DEFAULT_VALUE_MIN,
  DEFAULT_VALUE_MAX,
  MAX_THRESHOLDS,
  THRESHOLD_TYPE_BASE,
} from 'shared/constants/thresholds'
import {ErrorHandling} from 'src/shared/decorators/errors'

const formatColor = color => {
  const {hex, name} = color
  return {hex, name}
}

@ErrorHandling
class ThresholdsList extends Component {
  handleAddThreshold = () => {
    const {
      thresholdsListColors,
      thresholdsListType,
      handleUpdateThresholdsListColors,
      onResetFocus,
    } = this.props

    const randomColor = _.random(0, THRESHOLD_COLORS.length - 1)

    const maxValue = DEFAULT_VALUE_MIN
    const minValue = DEFAULT_VALUE_MAX

    let randomValue = _.round(_.random(minValue, maxValue, true), 2)

    if (thresholdsListColors.length > 0) {
      const colorsValues = _.mapValues(thresholdsListColors, 'value')
      do {
        randomValue = _.round(_.random(minValue, maxValue, true), 2)
      } while (_.includes(colorsValues, randomValue))
    }

    const newThreshold = {
      type: thresholdsListType,
      id: uuid.v4(),
      value: randomValue,
      hex: THRESHOLD_COLORS[randomColor].hex,
      name: THRESHOLD_COLORS[randomColor].name,
    }

    const updatedColors = _.sortBy(
      [...thresholdsListColors, newThreshold],
      color => color.value
    )

    handleUpdateThresholdsListColors(updatedColors)
    onResetFocus()
  }

  handleDeleteThreshold = threshold => {
    const {
      handleUpdateThresholdsListColors,
      onResetFocus,
      thresholdsListColors,
    } = this.props
    const updatedThresholdsListColors = thresholdsListColors.filter(
      color => color.id !== threshold.id
    )
    const sortedColors = _.sortBy(
      updatedThresholdsListColors,
      color => color.value
    )

    handleUpdateThresholdsListColors(sortedColors)
    onResetFocus()
  }

  handleChooseColor = threshold => chosenColor => {
    const {handleUpdateThresholdsListColors} = this.props

    const thresholdsListColors = this.props.thresholdsListColors.map(
      color =>
        color.id === threshold.id
          ? {...color, hex: chosenColor.hex, name: chosenColor.name}
          : color
    )

    handleUpdateThresholdsListColors(thresholdsListColors)
  }

  handleUpdateColorValue = (threshold, value) => {
    const {handleUpdateThresholdsListColors} = this.props

    const thresholdsListColors = this.props.thresholdsListColors.map(
      color => (color.id === threshold.id ? {...color, value} : color)
    )

    handleUpdateThresholdsListColors(thresholdsListColors)
  }

  handleValidateColorValue = (threshold, targetValue) => {
    const {thresholdsListColors} = this.props
    const sortedColors = _.sortBy(thresholdsListColors, color => color.value)

    return !sortedColors.some(color => color.value === targetValue)
  }

  get sortedColors() {
    const {thresholdsListColors} = this.props
    const sortedColors = _.sortBy(thresholdsListColors, 'value')

    return sortedColors
  }

  render() {
    const {thresholdsListColors, showListHeading} = this.props
    const disableAddThreshold = thresholdsListColors.length > MAX_THRESHOLDS

    const thresholdsListClass = `thresholds-list${
      showListHeading ? ' graph-options-group' : ''
    }`

    return (
      <div className={thresholdsListClass}>
        {showListHeading && <label className="form-label">Thresholds</label>}
        <button
          className="btn btn-sm btn-primary"
          onClick={this.handleAddThreshold}
          disabled={disableAddThreshold}
        >
          <span className="icon plus" /> Add Threshold
        </button>
        {this.sortedColors.map(
          color =>
            color.id === THRESHOLD_TYPE_BASE ? (
              <div className="threshold-item" key={uuid.v4()}>
                <div className="threshold-item--label">Base Color</div>
                <ColorDropdown
                  colors={THRESHOLD_COLORS}
                  selected={formatColor(color)}
                  onChoose={this.handleChooseColor(color)}
                  stretchToFit={true}
                />
              </div>
            ) : (
              <Threshold
                visualizationType="single-stat"
                threshold={color}
                key={uuid.v4()}
                onChooseColor={this.handleChooseColor}
                onValidateColorValue={this.handleValidateColorValue}
                onUpdateColorValue={this.handleUpdateColorValue}
                onDeleteThreshold={this.handleDeleteThreshold}
              />
            )
        )}
      </div>
    )
  }
}
const {bool, func, string} = PropTypes

ThresholdsList.defaultProps = {
  showListHeading: false,
}
ThresholdsList.propTypes = {
  thresholdsListType: string.isRequired,
  thresholdsListColors: colorsNumberSchema.isRequired,
  handleUpdateThresholdsListColors: func.isRequired,
  onResetFocus: func.isRequired,
  showListHeading: bool,
}

const mapStateToProps = ({
  cellEditorOverlay: {thresholdsListType, thresholdsListColors},
}) => ({
  thresholdsListType,
  thresholdsListColors,
})

const mapDispatchToProps = dispatch => ({
  handleUpdateThresholdsListColors: bindActionCreators(
    updateThresholdsListColors,
    dispatch
  ),
})
export default connect(mapStateToProps, mapDispatchToProps)(ThresholdsList)
