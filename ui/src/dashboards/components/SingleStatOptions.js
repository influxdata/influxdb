import React, {PropTypes} from 'react'
import _ from 'lodash'

import FancyScrollbar from 'shared/components/FancyScrollbar'
import Threshold from 'src/dashboards/components/Threshold'
import ColorDropdown from 'shared/components/ColorDropdown'

import {
  GAUGE_COLORS,
  MAX_THRESHOLDS,
  SINGLE_STAT_BASE,
  SINGLE_STAT_TEXT,
  SINGLE_STAT_BG,
} from 'src/dashboards/constants/gaugeColors'

const formatColor = color => {
  const {hex, name} = color
  return {hex, name}
}
const SingleStatOptions = ({
  suffix,
  onSetSuffix,
  colors,
  onAddThreshold,
  onDeleteThreshold,
  onChooseColor,
  onValidateColorValue,
  onUpdateColorValue,
  singleStatColoration,
  onToggleSingleStatColoration,
}) => {
  const disableAddThreshold = colors.length > MAX_THRESHOLDS

  const sortedColors = _.sortBy(colors, color => Number(color.value))

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
            onClick={onAddThreshold}
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
                      onChoose={onChooseColor(color)}
                      stretchToFit={true}
                    />
                  </div>
                : <Threshold
                    visualizationType="single-stat"
                    threshold={color}
                    key={color.id}
                    onChooseColor={onChooseColor}
                    onValidateColorValue={onValidateColorValue}
                    onUpdateColorValue={onUpdateColorValue}
                    onDeleteThreshold={onDeleteThreshold}
                  />
          )}
        </div>
        <div className="single-stat-controls">
          <div className="form-group col-xs-6">
            <label>Coloring</label>
            <ul className="nav nav-tablist nav-tablist-sm">
              <li
                className={
                  singleStatColoration === SINGLE_STAT_BG ? 'active' : null
                }
                onClick={onToggleSingleStatColoration(SINGLE_STAT_BG)}
              >
                Background
              </li>
              <li
                className={
                  singleStatColoration === SINGLE_STAT_TEXT ? 'active' : null
                }
                onClick={onToggleSingleStatColoration(SINGLE_STAT_TEXT)}
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

const {arrayOf, func, shape, string} = PropTypes

SingleStatOptions.defaultProps = {
  colors: [],
}

SingleStatOptions.propTypes = {
  colors: arrayOf(
    shape({
      type: string.isRequired,
      hex: string.isRequired,
      id: string.isRequired,
      name: string.isRequired,
      value: string.isRequired,
    }).isRequired
  ),
  onAddThreshold: func.isRequired,
  onDeleteThreshold: func.isRequired,
  onChooseColor: func.isRequired,
  onValidateColorValue: func.isRequired,
  onUpdateColorValue: func.isRequired,
  singleStatColoration: string.isRequired,
  onToggleSingleStatColoration: func.isRequired,
  onSetSuffix: func.isRequired,
  suffix: string.isRequired,
}

export default SingleStatOptions
