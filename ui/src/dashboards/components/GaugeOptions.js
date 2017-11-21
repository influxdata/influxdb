import React, {Component, PropTypes} from 'react'

import FancyScrollbar from 'shared/components/FancyScrollbar'
import ColorDropdown from 'shared/components/ColorDropdown'
import uuid from 'node-uuid'

import {
  MAX_THRESHOLDS,
  GAUGE_COLORS,
} from 'src/dashboards/constants/gaugeColors'

class GaugeOptions extends Component {
  constructor(props) {
    super(props)

    this.state = {
      minValue: this.props.config.minValue,
      minColor: this.props.config.minColor,
      maxValue: this.props.config.maxValue,
      maxColor: this.props.config.maxColor,
      thresholds: this.props.config.thresholds,
    }
  }

  handleChooseMinColor = color => {
    this.setState({minColor: color})
  }

  handleChooseMaxColor = color => {
    this.setState({maxColor: color})
  }

  handleAddThreshold = () => {
    const {minValue, thresholds} = this.state
    const randomColor = Math.floor(Math.random() * GAUGE_COLORS.length)

    const newThreshold = {
      id: uuid.v4(),
      value: minValue + 1,
      hex: GAUGE_COLORS[randomColor].hex,
      text: GAUGE_COLORS[randomColor].text,
    }
    thresholds.push(newThreshold)

    this.setState([thresholds])
  }

  handleDeleteThreshold = threshold => () => {
    const {thresholds} = this.state

    const filteredThresholds = thresholds.filter(t => {
      return t.id !== threshold.id
    })

    this.setState({thresholds: filteredThresholds})
  }

  handleChooseColor = threshold => newColor => {
    threshold.hex = newColor.hex
    threshold.text = newColor.text
  }

  handleChangeValue = threshold => e => {
    threshold.value = Number(e.target.value)
  }

  render() {
    const {minValue, minColor, maxValue, maxColor, thresholds} = this.state

    const disableMaxColor = thresholds.length > 0

    const disableAddThreshold = thresholds.length > MAX_THRESHOLDS

    return (
      <FancyScrollbar
        className="display-options--cell y-axis-controls"
        autoHide={false}
      >
        <div className="display-options--cell-wrapper">
          <h5 className="display-options--header">Gauge Controls</h5>
          <div className="gauge-controls">
            <div className="gauge-controls--section">
              <div className="gauge-controls--label">Minimum</div>
              <input
                defaultValue={minValue}
                className="form-control input-sm gauge-controls--input"
                type="number"
                placeholder=">= 0"
                min={0}
              />
              <ColorDropdown
                colors={GAUGE_COLORS}
                selected={minColor}
                onChoose={this.handleChooseMinColor}
              />
            </div>
            {thresholds.length > 0 &&
              thresholds.map(threshold =>
                <div className="gauge-controls--section" key={threshold.id}>
                  <div className="gauge-controls--label-editable">
                    Threshold
                  </div>
                  <button
                    className="btn btn-default btn-sm btn-square gauge-controls--delete"
                    onClick={this.handleDeleteThreshold(threshold)}
                  >
                    <span className="icon remove" />
                  </button>
                  <input
                    defaultValue={threshold.value}
                    className="form-control input-sm gauge-controls--input"
                    type="number"
                    onChange={this.handleChangeValue(threshold)}
                    min={minValue + 1}
                    max={maxValue - 1}
                  />
                  <ColorDropdown
                    colors={GAUGE_COLORS}
                    selected={threshold}
                    onChoose={this.handleChooseColor(threshold)}
                  />
                </div>
              )}
            <div className="gauge-controls--section">
              <div className="gauge-controls--label">Maximum</div>
              <input
                defaultValue={maxValue}
                className="form-control input-sm gauge-controls--input"
                type="number"
              />
              <ColorDropdown
                colors={GAUGE_COLORS}
                selected={maxColor}
                onChoose={this.handleChooseMaxColor}
                disabled={disableMaxColor}
              />
            </div>
            <button
              className="btn btn-sm btn-primary gauge-controls--add-threshold"
              onClick={this.handleAddThreshold}
              disabled={disableAddThreshold}
            >
              <span className="icon plus" /> Add Threshold
            </button>
          </div>
        </div>
      </FancyScrollbar>
    )
  }
}

const {arrayOf, func, number, shape, string} = PropTypes

GaugeOptions.defaultProps = {
  config: {
    minValue: 0,
    minColor: GAUGE_COLORS[2],
    maxValue: 100,
    maxColor: GAUGE_COLORS[13],
    thresholds: [],
  },
}

GaugeOptions.propTypes = {
  config: shape({
    minValue: number.isRequired,
    minColor: shape({hex: string.isRequired, text: string.isRequired})
      .isRequired,
    maxValue: number.isRequired,
    maxColor: shape({hex: string.isRequired, text: string.isRequired})
      .isRequired,
    thresholds: arrayOf(
      shape({
        value: number.isRequired,
        hex: string.isRequired,
        text: string.isRequired,
      })
    ),
  }),
  onUpdateOptions: func,
}

export default GaugeOptions
