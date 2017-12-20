import React, {PropTypes} from 'react'
import _ from 'lodash'

import FancyScrollbar from 'shared/components/FancyScrollbar'
import GaugeThreshold from 'src/dashboards/components/GaugeThreshold'

import {
  MAX_THRESHOLDS,
  MIN_THRESHOLDS,
  DEFAULT_COLORS,
} from 'src/dashboards/constants/gaugeColors'

const GaugeOptions = ({
  colors,
  onAddThreshold,
  onDeleteThreshold,
  onChooseColor,
  onValidateColorValue,
  onUpdateColorValue,
}) => {
  const disableMaxColor = colors.length > MIN_THRESHOLDS

  const disableAddThreshold = colors.length > MAX_THRESHOLDS

  const sortedColors = _.sortBy(colors, color => Number(color.value))

  return (
    <FancyScrollbar
      className="display-options--cell y-axis-controls"
      autoHide={false}
    >
      <div className="display-options--cell-wrapper">
        <h5 className="display-options--header">Gauge Controls</h5>
        <div className="gauge-controls">
          {sortedColors.map(color =>
            <GaugeThreshold
              threshold={color}
              key={color.id}
              disableMaxColor={disableMaxColor}
              onChooseColor={onChooseColor}
              onValidateColorValue={onValidateColorValue}
              onUpdateColorValue={onUpdateColorValue}
              onDeleteThreshold={onDeleteThreshold}
            />
          )}
          <button
            className="btn btn-sm btn-primary gauge-controls--add-threshold"
            onClick={onAddThreshold}
            disabled={disableAddThreshold}
          >
            <span className="icon plus" /> Add Threshold
          </button>
        </div>
      </div>
    </FancyScrollbar>
  )
}

const {arrayOf, func, shape, string} = PropTypes

GaugeOptions.defaultProps = {
  colors: DEFAULT_COLORS,
}

GaugeOptions.propTypes = {
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
}

export default GaugeOptions
