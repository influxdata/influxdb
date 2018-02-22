import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import OptIn from 'shared/components/OptIn'
import Input from 'src/dashboards/components/DisplayOptionsInput'
import {Tabber, Tab} from 'src/dashboards/components/Tabber'
import FancyScrollbar from 'shared/components/FancyScrollbar'

import {DISPLAY_OPTIONS, TOOLTIP_CONTENT} from 'src/dashboards/constants'
import {GRAPH_TYPES} from 'src/dashboards/graphics/graph'

const {LINEAR, LOG, BASE_2, BASE_10} = DISPLAY_OPTIONS
const getInputMin = scale => (scale === LOG ? '0' : null)

import {updateAxes} from 'src/dashboards/actions/cellEditorOverlay'

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
    } = this.props

    const [min, max] = bounds

    const {menuOption} = GRAPH_TYPES.find(graph => graph.type === type)

    return (
      <FancyScrollbar
        className="display-options--cell y-axis-controls"
        autoHide={false}
      >
        <div className="display-options--cell-wrapper">
          <h5 className="display-options--header">
            {menuOption} Controls
          </h5>
          <form autoComplete="off" style={{margin: '0 -6px'}}>
            <div className="form-group col-sm-12">
              <label htmlFor="prefix">Title</label>
              <OptIn
                customPlaceholder={defaultYLabel || 'y-axis title'}
                customValue={label}
                onSetValue={this.handleSetLabel}
                type="text"
              />
            </div>
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
            />
            <Input
              name="suffix"
              id="suffix"
              value={suffix}
              labelText="Y-Value's Suffix"
              onChange={this.handleSetPrefixSuffix}
            />
            <Tabber
              labelText="Y-Value's Format"
              tipID="Y-Values's Format"
              tipContent={TOOLTIP_CONTENT.FORMAT}
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
          </form>
        </div>
      </FancyScrollbar>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

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
