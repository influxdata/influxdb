import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import OptIn from 'shared/components/OptIn'
import Input from 'src/dashboards/components/DisplayOptionsInput'
import {Tabber, Tab} from 'src/dashboards/components/Tabber'
import FancyScrollbar from 'shared/components/FancyScrollbar'
import LineGraphColorSelector from 'src/shared/components/LineGraphColorSelector'

import {
  AXES_SCALE_OPTIONS,
  TOOLTIP_Y_VALUE_FORMAT,
} from 'src/dashboards/constants/cellEditor'
import {GRAPH_TYPES} from 'src/dashboards/graphics/graph'

import {updateAxes} from 'src/dashboards/actions/cellEditorOverlay'
import {ErrorHandling} from 'src/shared/decorators/errors'

const {LINEAR, LOG, BASE_2, BASE_10} = AXES_SCALE_OPTIONS
const getInputMin = scale => (scale === LOG ? '0' : null)

@ErrorHandling
class AxesOptions extends Component {
  handleSetPrefixSuffix = e => {
    const {handleUpdateAxes, axes} = this.props
    const {prefix, suffix} = e.target.form

    const newAxes = {
      ...axes,
      y: {
        ...axes.y,
        prefix: prefix.value,
        suffix: suffix.value,
      },
    }

    handleUpdateAxes(newAxes)
  }

  handleSetYAxisBoundMin = min => {
    const {handleUpdateAxes, axes} = this.props
    const {y: {bounds: [, max]}} = this.props.axes
    const newAxes = {...axes, y: {...axes.y, bounds: [min, max]}}

    handleUpdateAxes(newAxes)
  }

  handleSetYAxisBoundMax = max => {
    const {handleUpdateAxes, axes} = this.props
    const {y: {bounds: [min]}} = axes
    const newAxes = {...axes, y: {...axes.y, bounds: [min, max]}}

    handleUpdateAxes(newAxes)
  }

  handleSetLabel = label => {
    const {handleUpdateAxes, axes} = this.props
    const newAxes = {...axes, y: {...axes.y, label}}

    handleUpdateAxes(newAxes)
  }

  handleSetScale = scale => () => {
    const {handleUpdateAxes, axes} = this.props
    const newAxes = {...axes, y: {...axes.y, scale}}

    handleUpdateAxes(newAxes)
  }

  handleSetBase = base => () => {
    const {handleUpdateAxes, axes} = this.props
    const newAxes = {...axes, y: {...axes.y, base}}

    handleUpdateAxes(newAxes)
  }

  render() {
    const {
      axes: {y: {bounds, label, prefix, suffix, base, scale, defaultYLabel}},
      type,
      staticLegend,
      onToggleStaticLegend,
    } = this.props

    const [min, max] = bounds

    const {menuOption} = GRAPH_TYPES.find(graph => graph.type === type)

    return (
      <FancyScrollbar
        className="display-options--cell y-axis-controls"
        autoHide={false}
      >
        <div className="display-options--cell-wrapper">
          <h5 className="display-options--header">{menuOption} Controls</h5>
          <form autoComplete="off" className="form-group-wrapper">
            <div className="form-group col-sm-12">
              <label htmlFor="prefix">Title</label>
              <OptIn
                customPlaceholder={defaultYLabel || 'y-axis title'}
                customValue={label}
                onSetValue={this.handleSetLabel}
                type="text"
              />
            </div>
            <LineGraphColorSelector />
            <div className="form-group col-sm-6">
              <label htmlFor="min">Min</label>
              <OptIn
                customPlaceholder={'min'}
                customValue={min}
                onSetValue={this.handleSetYAxisBoundMin}
                type="number"
                min={getInputMin(scale)}
              />
            </div>
            <div className="form-group col-sm-6">
              <label htmlFor="max">Max</label>
              <OptIn
                customPlaceholder={'max'}
                customValue={max}
                onSetValue={this.handleSetYAxisBoundMax}
                type="number"
                min={getInputMin(scale)}
              />
            </div>
            <Input
              name="prefix"
              id="prefix"
              value={prefix}
              labelText="Y-Value's Prefix"
              onChange={this.handleSetPrefixSuffix}
              maxLength="5"
            />
            <Input
              name="suffix"
              id="suffix"
              value={suffix}
              labelText="Y-Value's Suffix"
              onChange={this.handleSetPrefixSuffix}
              maxLength="5"
            />
            <Tabber
              labelText="Y-Value's Format"
              tipID="Y-Values's Format"
              tipContent={TOOLTIP_Y_VALUE_FORMAT}
            >
              <Tab
                text="K/M/B"
                isActive={base === BASE_10}
                onClickTab={this.handleSetBase(BASE_10)}
              />
              <Tab
                text="K/M/G"
                isActive={base === BASE_2}
                onClickTab={this.handleSetBase(BASE_2)}
              />
            </Tabber>
            <Tabber labelText="Scale">
              <Tab
                text="Linear"
                isActive={scale === LINEAR}
                onClickTab={this.handleSetScale(LINEAR)}
              />
              <Tab
                text="Logarithmic"
                isActive={scale === LOG}
                onClickTab={this.handleSetScale(LOG)}
              />
            </Tabber>
            <Tabber labelText="Static Legend">
              <Tab
                text="Show"
                isActive={staticLegend}
                onClickTab={onToggleStaticLegend(true)}
              />
              <Tab
                text="Hide"
                isActive={!staticLegend}
                onClickTab={onToggleStaticLegend(false)}
              />
            </Tabber>
          </form>
        </div>
      </FancyScrollbar>
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

AxesOptions.defaultProps = {
  axes: {
    y: {
      bounds: ['', ''],
      prefix: '',
      suffix: '',
      base: BASE_10,
      scale: LINEAR,
      defaultYLabel: '',
    },
  },
}

AxesOptions.propTypes = {
  type: string.isRequired,
  axes: shape({
    y: shape({
      bounds: arrayOf(string),
      label: string,
      defaultYLabel: string,
    }),
  }).isRequired,
  onToggleStaticLegend: func.isRequired,
  staticLegend: bool,
  handleUpdateAxes: func.isRequired,
}

const mapStateToProps = ({cellEditorOverlay: {cell: {axes, type}}}) => ({
  axes,
  type,
})

const mapDispatchToProps = dispatch => ({
  handleUpdateAxes: bindActionCreators(updateAxes, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(AxesOptions)
