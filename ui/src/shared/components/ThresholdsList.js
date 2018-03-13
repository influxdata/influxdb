import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import _ from 'lodash'
import uuid from 'uuid'

import Threshold from 'src/dashboards/components/Threshold'
import ColorDropdown from 'shared/components/ColorDropdown'

import {updateSingleStatColors} from 'src/dashboards/actions/cellEditorOverlay'

import {
  GAUGE_COLORS,
  DEFAULT_VALUE_MIN,
  DEFAULT_VALUE_MAX,
  MAX_THRESHOLDS,
  SINGLE_STAT_BASE,
} from 'shared/constants/thresholds'

const formatColor = color => {
  const {hex, name} = color
  return {hex, name}
}

class ThresholdsList extends Component {
  handleAddThreshold = () => {
    const {
      singleStatColors,
      singleStatType,
      handleUpdateSingleStatColors,
      onResetFocus,
    } = this.props

    const randomColor = _.random(0, GAUGE_COLORS.length - 1)

    const maxValue = DEFAULT_VALUE_MIN
    const minValue = DEFAULT_VALUE_MAX

    let randomValue = _.round(_.random(minValue, maxValue, true), 2)

    if (singleStatColors.length > 0) {
      const colorsValues = _.mapValues(singleStatColors, 'value')
      do {
        randomValue = _.round(_.random(minValue, maxValue, true), 2)
      } while (_.includes(colorsValues, randomValue))
    }

    const newThreshold = {
      type: singleStatType,
      id: uuid.v4(),
      value: randomValue,
      hex: GAUGE_COLORS[randomColor].hex,
      name: GAUGE_COLORS[randomColor].name,
    }

    const updatedColors = _.sortBy(
      [...singleStatColors, newThreshold],
      color => color.value
    )

    handleUpdateSingleStatColors(updatedColors)
    onResetFocus()
  }

  handleDeleteThreshold = threshold => () => {
    const {handleUpdateSingleStatColors, onResetFocus} = this.props
    const singleStatColors = this.props.singleStatColors.filter(
      color => color.id !== threshold.id
    )
    const sortedColors = _.sortBy(singleStatColors, color => color.value)

    handleUpdateSingleStatColors(sortedColors)
    onResetFocus()
  }

  handleChooseColor = threshold => chosenColor => {
    const {handleUpdateSingleStatColors} = this.props

    const singleStatColors = this.props.singleStatColors.map(
      color =>
        color.id === threshold.id
          ? {...color, hex: chosenColor.hex, name: chosenColor.name}
          : color
    )

    handleUpdateSingleStatColors(singleStatColors)
  }

  handleUpdateColorValue = (threshold, value) => {
    const {handleUpdateSingleStatColors} = this.props

    const singleStatColors = this.props.singleStatColors.map(
      color => (color.id === threshold.id ? {...color, value} : color)
    )

    handleUpdateSingleStatColors(singleStatColors)
  }

  handleValidateColorValue = (threshold, targetValue) => {
    const {singleStatColors} = this.props
    const sortedColors = _.sortBy(singleStatColors, color => color.value)

    return !sortedColors.some(color => color.value === targetValue)
  }

  handleSortColors = () => {
    const {singleStatColors, handleUpdateSingleStatColors} = this.props
    const sortedColors = _.sortBy(singleStatColors, color => color.value)

    handleUpdateSingleStatColors(sortedColors)
  }

  render() {
    const {singleStatColors} = this.props
    const disableAddThreshold = singleStatColors.length > MAX_THRESHOLDS

    return (
      <div className="thresholds-list">
        <button
          className="btn btn-sm btn-primary"
          onClick={this.handleAddThreshold}
          disabled={disableAddThreshold}
        >
          <span className="icon plus" /> Add Threshold
        </button>
        {singleStatColors.map(
          color =>
            color.id === SINGLE_STAT_BASE
              ? <div className="threshold-item" key={color.id}>
                  <div className="threshold-item--label">Base Color</div>
                  <ColorDropdown
                    colors={GAUGE_COLORS}
                    selected={formatColor(color)}
                    onChoose={this.handleChooseColor(color)}
                    stretchToFit={true}
                  />
                </div>
              : <Threshold
                  visualizationType="single-stat"
                  threshold={color}
                  key={color.id}
                  onChooseColor={this.handleChooseColor}
                  onValidateColorValue={this.handleValidateColorValue}
                  onUpdateColorValue={this.handleUpdateColorValue}
                  onDeleteThreshold={this.handleDeleteThreshold}
                  onSortColors={this.handleSortColors}
                />
        )}
      </div>
    )
  }
}
const {arrayOf, func, number, shape, string} = PropTypes

ThresholdsList.propTypes = {
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
  handleUpdateSingleStatColors: func.isRequired,
  onResetFocus: func.isRequired,
}

const mapStateToProps = ({
  cellEditorOverlay: {singleStatType, singleStatColors},
}) => ({
  singleStatType,
  singleStatColors,
})

const mapDispatchToProps = dispatch => ({
  handleUpdateSingleStatColors: bindActionCreators(
    updateSingleStatColors,
    dispatch
  ),
})
export default connect(mapStateToProps, mapDispatchToProps)(ThresholdsList)
