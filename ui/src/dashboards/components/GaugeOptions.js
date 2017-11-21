import React, {PropTypes} from 'react'

import FancyScrollbar from 'shared/components/FancyScrollbar'
import GaugeThreshold from 'src/dashboards/components/GaugeThreshold'

import {
  COLOR_TYPE_MIN,
  DEFAULT_VALUE_MIN,
  COLOR_TYPE_MAX,
  DEFAULT_VALUE_MAX,
  MAX_THRESHOLDS,
  MIN_THRESHOLDS,
  GAUGE_COLORS,
} from 'src/dashboards/constants/gaugeColors'

const GaugeOptions = ({
  colors,
  onAddThreshold,
  onDeleteThreshold,
  onChooseColor,
  onChangeValue,
}) => {
  const disableMaxColor = colors.length > MIN_THRESHOLDS

  const disableAddThreshold = colors.length > MAX_THRESHOLDS

  return (
    <FancyScrollbar
      className="display-options--cell y-axis-controls"
      autoHide={false}
    >
      <div className="display-options--cell-wrapper">
        <h5 className="display-options--header">Gauge Controls</h5>
        <div className="gauge-controls">
          {colors.map(color =>
            <GaugeThreshold
              threshold={color}
              key={color.id}
              disableMaxColor={disableMaxColor}
              onChooseColor={onChooseColor}
              onChangeValue={onChangeValue}
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
  colors: [
    {
      type: COLOR_TYPE_MIN,
      hex: GAUGE_COLORS[11].hex,
      id: '0',
      name: GAUGE_COLORS[11].name,
      value: DEFAULT_VALUE_MIN,
    },
    {
      type: COLOR_TYPE_MAX,
      hex: GAUGE_COLORS[14].hex,
      id: '1',
      name: GAUGE_COLORS[14].name,
      value: DEFAULT_VALUE_MAX,
    },
  ],
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
  onAddThreshold: func,
  onDeleteThreshold: func,
  onChooseColor: func,
  onChangeValue: func,
}

export default GaugeOptions
