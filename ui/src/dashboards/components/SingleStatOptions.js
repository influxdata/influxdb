import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import _ from 'lodash'
import uuid from 'node-uuid'

import FancyScrollbar from 'shared/components/FancyScrollbar'
import Threshold from 'src/dashboards/components/Threshold'
import ColorDropdown from 'shared/components/ColorDropdown'

import {
  GAUGE_COLORS,
  DEFAULT_VALUE_MIN,
  DEFAULT_VALUE_MAX,
  MAX_THRESHOLDS,
  SINGLE_STAT_BASE,
  SINGLE_STAT_TEXT,
  SINGLE_STAT_BG,
} from 'src/dashboards/constants/gaugeColors'

import {
  updateSingleStatType,
  updateSingleStatColors,
} from 'src/dashboards/actions/cellEditorOverlay'

const formatColor = color => {
  const {hex, name} = color
  return {hex, name}
}

class SingleStatOptions extends Component {
  handleToggleSingleStatType = newType => () => {
    const {handleUpdateSingleStatType} = this.props

    handleUpdateSingleStatType(newType)
  }

  handleAddThreshold = () => {
    const {
      singleStatColors,
      singleStatType,
      handleUpdateSingleStatColors,
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

    handleUpdateSingleStatColors([...singleStatColors, newThreshold])
  }

  handleDeleteThreshold = threshold => () => {
    const {handleUpdateSingleStatColors} = this.props

    const singleStatColors = this.props.singleStatColors.filter(
      color => color.id !== threshold.id
    )

    handleUpdateSingleStatColors(singleStatColors)
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

  render() {
    const {suffix, onSetSuffix, singleStatColors, singleStatType} = this.props

    const disableAddThreshold = singleStatColors.length > MAX_THRESHOLDS

    const sortedColors = _.sortBy(singleStatColors, color => color.value)

    return (
      <FancyScrollbar
        className="display-options--cell y-axis-controls"
        autoHide={false}
      >
        <div className="display-options--cell-wrapper">
          <h5 className="display-options--header">Single Stat Controls</h5>
          <div className="gauge-controls">
            <button
              className="btn btn-sm btn-primary gauge-controls--add-threshold"
              onClick={this.handleAddThreshold}
              disabled={disableAddThreshold}
            >
              <span className="icon plus" /> Add Threshold
            </button>
            {sortedColors.map(
              color =>
                color.id === SINGLE_STAT_BASE
                  ? <div className="gauge-controls--section" key={color.id}>
                      <div className="gauge-controls--label">Base Color</div>
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
                    />
            )}
          </div>
          <div className="single-stat-controls">
            <div className="form-group col-xs-6">
              <label>Coloring</label>
              <ul className="nav nav-tablist nav-tablist-sm">
                <li
                  className={`${singleStatType === SINGLE_STAT_BG
                    ? 'active'
                    : ''}`}
                  onClick={this.handleToggleSingleStatType(SINGLE_STAT_BG)}
                >
                  Background
                </li>
                <li
                  className={`${singleStatType === SINGLE_STAT_TEXT
                    ? 'active'
                    : ''}`}
                  onClick={this.handleToggleSingleStatType(SINGLE_STAT_TEXT)}
                >
                  Text
                </li>
              </ul>
            </div>
            <div className="form-group col-xs-6">
              <label>Suffix</label>
              <input
                className="form-control input-sm"
                placeholder="%, MPH, etc."
                defaultValue={suffix}
                onChange={onSetSuffix}
                maxLength="5"
              />
            </div>
          </div>
        </div>
      </FancyScrollbar>
    )
  }
}

const {arrayOf, func, number, shape, string} = PropTypes

SingleStatOptions.defaultProps = {
  colors: [],
}

SingleStatOptions.propTypes = {
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
  onSetSuffix: func.isRequired,
  suffix: string.isRequired,
  handleUpdateSingleStatType: func.isRequired,
  handleUpdateSingleStatColors: func.isRequired,
}

const mapStateToProps = ({
  cellEditorOverlay: {singleStatType, singleStatColors},
}) => ({
  singleStatType,
  singleStatColors,
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
})

export default connect(mapStateToProps, mapDispatchToProps)(SingleStatOptions)
