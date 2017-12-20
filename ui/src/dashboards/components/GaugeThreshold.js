import React, {Component, PropTypes} from 'react'

import ColorDropdown from 'shared/components/ColorDropdown'

import {
  COLOR_TYPE_MIN,
  COLOR_TYPE_MAX,
  GAUGE_COLORS,
} from 'src/dashboards/constants/gaugeColors'

class GaugeThreshold extends Component {
  constructor(props) {
    super(props)

    this.state = {
      workingValue: this.props.threshold.value,
      valid: true,
    }
  }

  handleChangeWorkingValue = e => {
    const {threshold, onValidateColorValue, onUpdateColorValue} = this.props

    const valid = onValidateColorValue(threshold, e)

    if (valid) {
      onUpdateColorValue(threshold, e.target.value)
    }

    this.setState({valid, workingValue: e.target.value})
  }

  handleBlur = () => {
    this.setState({workingValue: this.props.threshold.value, valid: true})
  }

  render() {
    const {
      threshold,
      threshold: {type, hex, name},
      disableMaxColor,
      onChooseColor,
      onDeleteThreshold,
    } = this.props
    const {workingValue, valid} = this.state
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

    const inputClass = valid
      ? 'form-control input-sm gauge-controls--input'
      : 'form-control input-sm gauge-controls--input form-volcano'

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
          value={workingValue}
          className={inputClass}
          type="number"
          onChange={this.handleChangeWorkingValue}
          onBlur={this.handleBlur}
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
  onValidateColorValue: func.isRequired,
  onUpdateColorValue: func.isRequired,
  onDeleteThreshold: func.isRequired,
}

export default GaugeThreshold
