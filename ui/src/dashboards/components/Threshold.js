import React, {Component, PropTypes} from 'react'

import ColorDropdown from 'shared/components/ColorDropdown'

import {GAUGE_COLORS} from 'shared/constants/thresholds'

class Threshold extends Component {
  constructor(props) {
    super(props)

    this.state = {
      workingValue: this.props.threshold.value,
      valid: true,
    }
  }

  handleChangeWorkingValue = e => {
    const {threshold, onValidateColorValue, onUpdateColorValue} = this.props
    const targetValue = Number(e.target.value)

    const valid = onValidateColorValue(threshold, targetValue)

    if (valid) {
      onUpdateColorValue(threshold, targetValue)
    }

    this.setState({valid, workingValue: targetValue})
  }

  handleBlur = () => {
    this.setState({workingValue: this.props.threshold.value, valid: true})
    this.props.onSortColors()
  }

  handleKeyUp = e => {
    if (e.key === 'Enter') {
      this.thresholdInputRef.blur()
    }
  }

  render() {
    const {
      visualizationType,
      threshold,
      threshold: {hex, name},
      disableMaxColor,
      onChooseColor,
      onDeleteThreshold,
      isMin,
      isMax,
    } = this.props
    const {workingValue, valid} = this.state
    const selectedColor = {hex, name}

    let label = 'Threshold'
    let labelClass = 'threshold-item--label__editable'
    let canBeDeleted = true

    if (visualizationType === 'gauge') {
      labelClass =
        isMin || isMax
          ? 'threshold-item--label'
          : 'threshold-item--label__editable'
      canBeDeleted = !(isMin || isMax)
    }

    if (isMin && visualizationType === 'gauge') {
      label = 'Minimum'
    }
    if (isMax && visualizationType === 'gauge') {
      label = 'Maximum'
    }

    const inputClass = valid
      ? 'form-control input-sm threshold-item--input'
      : 'form-control input-sm threshold-item--input form-volcano'

    return (
      <div className="threshold-item">
        <div className={labelClass}>
          {label}
        </div>
        {canBeDeleted
          ? <button
              className="btn btn-default btn-sm btn-square"
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
          onKeyUp={this.handleKeyUp}
          ref={r => (this.thresholdInputRef = r)}
        />
        <ColorDropdown
          colors={GAUGE_COLORS}
          selected={selectedColor}
          onChoose={onChooseColor(threshold)}
          disabled={isMax && disableMaxColor}
        />
      </div>
    )
  }
}

const {bool, func, number, shape, string} = PropTypes

Threshold.propTypes = {
  visualizationType: string.isRequired,
  threshold: shape({
    type: string.isRequired,
    hex: string.isRequired,
    id: string.isRequired,
    name: string.isRequired,
    value: number.isRequired,
  }).isRequired,
  disableMaxColor: bool,
  onChooseColor: func.isRequired,
  onValidateColorValue: func.isRequired,
  onUpdateColorValue: func.isRequired,
  onDeleteThreshold: func.isRequired,
  isMin: bool,
  isMax: bool,
  onSortColors: func.isRequired,
}

export default Threshold
