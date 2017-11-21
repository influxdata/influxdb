import React, {PropTypes} from 'react'

import ColorDropdown from 'shared/components/ColorDropdown'

import {
  COLOR_TYPE_MIN,
  COLOR_TYPE_MAX,
  GAUGE_COLORS,
} from 'src/dashboards/constants/gaugeColors'

const GaugeThreshold = ({
  threshold,
  threshold: {type, hex, name, value},
  disableMaxColor,
  onChooseColor,
  onChangeValue,
  onDeleteThreshold,
}) => {
  const selectedColor = {hex, name}

  const labelClass =
    type === COLOR_TYPE_MIN || type === COLOR_TYPE_MAX
      ? 'gauge-controls--label'
      : 'gauge-controls--label-editable'

  const canBeDeleted = !(type === COLOR_TYPE_MIN || type === COLOR_TYPE_MAX)

  let label = 'Threshold'
  if (type === COLOR_TYPE_MIN) {
    label = 'Minimum'
  }
  if (type === COLOR_TYPE_MAX) {
    label = 'Maximum'
  }

  return (
    <div className="gauge-controls--section">
      <div className={labelClass}>
        {label}
      </div>
      {canBeDeleted
        ? <button
            className="btn btn-default btn-sm btn-square gauge-controls--delete"
            onClick={onDeleteThreshold(threshold)}
          >
            <span className="icon remove" />
          </button>
        : null}
      <input
        value={value}
        className="form-control input-sm gauge-controls--input"
        type="number"
        onChange={onChangeValue(threshold)}
        min={0}
      />
      <ColorDropdown
        colors={GAUGE_COLORS}
        selected={selectedColor}
        onChoose={onChooseColor(threshold)}
        disabled={type === COLOR_TYPE_MAX && disableMaxColor}
      />
    </div>
  )
}

const {bool, func, shape, string} = PropTypes

GaugeThreshold.propTypes = {
  threshold: shape({
    type: string.isRequired,
    hex: string.isRequired,
    id: string.isRequired,
    name: string.isRequired,
    value: string.isRequired,
  }).isRequired,
  disableMaxColor: bool,
  onChooseColor: func.isRequired,
  onChangeValue: func.isRequired,
  onDeleteThreshold: func.isRequired,
}

export default GaugeThreshold
