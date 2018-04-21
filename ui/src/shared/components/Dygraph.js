/* eslint-disable no-magic-numbers */
import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import _ from 'lodash'
import NanoDate from 'nano-date'

import Dygraphs from 'src/external/dygraph'
import DygraphLegend from 'src/shared/components/DygraphLegend'
import StaticLegend from 'src/shared/components/StaticLegend'
import Annotations from 'src/shared/components/Annotations'
import Crosshair from 'src/shared/components/Crosshair'

import getRange, {getStackedRange} from 'shared/parsing/getRangeForDygraph'
import {AXES_SCALE_OPTIONS} from 'src/dashboards/constants/cellEditor'
import {buildDefaultYLabel} from 'shared/presenters'
import {numberValueFormatter} from 'src/utils/formatting'
import {NULL_HOVER_TIME} from 'src/shared/constants/tableGraph'
import {
  OPTIONS,
  LINE_COLORS,
  LABEL_WIDTH,
  CHAR_PIXELS,
  barPlotter,
} from 'src/shared/graphs/helpers'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {getLineColorsHexes} from 'src/shared/constants/graphColorPalettes'
const {LINEAR, LOG, BASE_10, BASE_2} = AXES_SCALE_OPTIONS

import {colorsStringSchema} from 'shared/schemas'

@ErrorHandling
class Dygraph extends Component {
  constructor(props) {
    super(props)
    this.state = {
      staticLegendHeight: null,
    }
  }

  componentDidMount() {
    const {
      axes: {y, y2},
      isGraphFilled: fillGraph,
      isBarGraph,
      options,
    } = this.props

    const timeSeries = this.timeSeries
    const graphRef = this.graphRef

    let defaultOptions = {
      fillGraph,
      logscale: y.scale === LOG,
      colors: this.lineColors,
      series: this.colorDygraphSeries,
      axes: {
        y: {
          valueRange: this.getYRange(timeSeries),
          axisLabelFormatter: (yval, __, opts) =>
            numberValueFormatter(yval, opts, y.prefix, y.suffix),
          axisLabelWidth: this.labelWidth,
          labelsKMB: y.base === BASE_10,
          labelsKMG2: y.base === BASE_2,
        },
        y2: {
          valueRange: getRange(timeSeries, y2.bounds),
        },
      },
      zoomCallback: (lower, upper) => this.handleZoom(lower, upper),
      highlightCircleSize: 0,
    }

    if (isBarGraph) {
      defaultOptions = {
        ...defaultOptions,
        plotter: barPlotter,
      }
    }

    this.dygraph = new Dygraphs(graphRef, timeSeries, {
      ...defaultOptions,
      ...options,
      ...OPTIONS,
    })

    const {w} = this.dygraph.getArea()
    this.props.setResolution(w)
  }

  componentWillUnmount() {
    this.dygraph.destroy()
    delete this.dygraph
  }

  shouldComponentUpdate(nextProps, nextState) {
    const arePropsEqual = _.isEqual(this.props, nextProps)
    const areStatesEqual = _.isEqual(this.state, nextState)
    return !arePropsEqual || !areStatesEqual
  }

  componentDidUpdate(prevProps) {
    const {labels, axes: {y, y2}, options, isBarGraph} = this.props

    const dygraph = this.dygraph

    if (!dygraph) {
      throw new Error(
        'Dygraph not configured in time; this should not be possible!'
      )
    }

    const timeSeries = this.timeSeries

    const timeRangeChanged = !_.isEqual(
      prevProps.timeRange,
      this.props.timeRange
    )

    if (this.dygraph.isZoomed() && timeRangeChanged) {
      this.dygraph.resetZoom()
    }

    const updateOptions = {
      ...options,
      labels,
      file: timeSeries,
      logscale: y.scale === LOG,
      ylabel: this.getLabel('y'),
      axes: {
        y: {
          valueRange: this.getYRange(timeSeries),
          axisLabelFormatter: (yval, __, opts) =>
            numberValueFormatter(yval, opts, y.prefix, y.suffix),
          axisLabelWidth: this.labelWidth,
          labelsKMB: y.base === BASE_10,
          labelsKMG2: y.base === BASE_2,
        },
        y2: {
          valueRange: getRange(timeSeries, y2.bounds),
        },
      },
      colors: this.lineColors,
      series: this.colorDygraphSeries,
      plotter: isBarGraph ? barPlotter : null,
    }

    dygraph.updateOptions(updateOptions)

    const {w} = this.dygraph.getArea()
    this.props.setResolution(w)
    this.resize()
  }

  getYRange = timeSeries => {
    const {options, axes: {y}, ruleValues} = this.props

    if (options.stackedGraph) {
      return getStackedRange(y.bounds)
    }

    const range = getRange(timeSeries, y.bounds, ruleValues)
    const [min, max] = range

    // Bug in Dygraph calculates a negative range for logscale when min range is 0
    if (y.scale === LOG && timeSeries.length === 1 && min <= 0) {
      return [0.1, max]
    }

    return range
  }

  handleZoom = (lower, upper) => {
    const {onZoom} = this.props

    if (this.dygraph.isZoomed() === false) {
      return onZoom(null, null)
    }

    onZoom(this.formatTimeRange(lower), this.formatTimeRange(upper))
  }

  get colorDygraphSeries() {
    const {dygraphSeries, colors, overrideLineColors} = this.props
    const numSeries = Object.keys(dygraphSeries).length
    const dygraphSeriesKeys = Object.keys(dygraphSeries).sort()
    let lineColors = getLineColorsHexes(colors, numSeries)

    if (overrideLineColors) {
      lineColors = getLineColorsHexes(overrideLineColors, numSeries)
    }

    const coloredDygraphSeries = {}
    for (const seriesName in dygraphSeries) {
      const series = dygraphSeries[seriesName]
      const color = lineColors[dygraphSeriesKeys.indexOf(seriesName)]

      coloredDygraphSeries[seriesName] = {...series, color}
    }
    return coloredDygraphSeries
  }

  eventToTimestamp = ({pageX: pxBetweenMouseAndPage}) => {
    const {left: pxBetweenGraphAndPage} = this.graphRef.getBoundingClientRect()
    const graphXCoordinate = pxBetweenMouseAndPage - pxBetweenGraphAndPage
    const timestamp = this.dygraph.toDataXCoord(graphXCoordinate)
    const [xRangeStart] = this.dygraph.xAxisRange()
    const clamped = Math.max(xRangeStart, timestamp)
    return `${clamped}`
  }

  handleHideLegend = () => {
    this.props.handleSetHoverTime(NULL_HOVER_TIME)
  }

  handleShowLegend = e => {
    const newTime = this.eventToTimestamp(e)
    this.props.handleSetHoverTime(newTime)
  }

  get lineColors() {
    return [...(this.props.overrideLineColors || LINE_COLORS)]
  }

  get labelWidth() {
    const {axes: {y}} = this.props
    return (
      LABEL_WIDTH +
      y.prefix.length * CHAR_PIXELS +
      y.suffix.length * CHAR_PIXELS
    )
  }

  get timeSeries() {
    const {timeSeries} = this.props
    // Avoid 'Can't plot empty data set' errors by falling back to a
    // default dataset that's valid for Dygraph.
    return timeSeries.length ? timeSeries : [[0]]
  }

  getLabel = axis => {
    const {axes, queries} = this.props
    const label = _.get(axes, [axis, 'label'], '')
    const queryConfig = _.get(queries, ['0', 'queryConfig'], false)

    if (label || !queryConfig) {
      return label
    }

    return buildDefaultYLabel(queryConfig)
  }

  resize = () => {
    this.dygraph.resizeElements_()
    this.dygraph.predraw_()
    this.dygraph.resize()
  }

  formatTimeRange = timeRange => {
    if (!timeRange) {
      return ''
    }
    const date = new NanoDate(timeRange)
    return date.toISOString()
  }

  handleReceiveStaticLegendHeight = staticLegendHeight => {
    this.setState({staticLegendHeight})
  }

  get areAnnotationsVisible() {
    if (!this.dygraph) {
      return false
    }

    const [start, end] = this.dygraph && this.dygraph.xAxisRange()
    return !!start && !!end
  }

  render() {
    const {staticLegendHeight} = this.state
    const {staticLegend, children, cellID} = this.props
    const nestedGraph = (children && children.length && children[0]) || children
    let dygraphStyle = {...this.props.containerStyle, zIndex: '2'}
    if (staticLegend) {
      const cellVerticalPadding = 16

      dygraphStyle = {
        ...this.props.containerStyle,
        zIndex: '2',
        height: `calc(100% - ${staticLegendHeight + cellVerticalPadding}px)`,
      }
    }

    return (
      <div className="dygraph-child">
        {this.dygraph && (
          <div className="dygraph-addons">
            {this.areAnnotationsVisible && (
              <Annotations
                dygraph={this.dygraph}
                dWidth={this.dygraph.width_}
                staticLegendHeight={staticLegendHeight}
              />
            )}
            <DygraphLegend
              cellID={cellID}
              dygraph={this.dygraph}
              onHide={this.handleHideLegend}
              onShow={this.handleShowLegend}
            />
            <Crosshair
              dygraph={this.dygraph}
              staticLegendHeight={staticLegendHeight}
            />
          </div>
        )}
        <div
          onMouseEnter={this.handleShowLegend}
          ref={r => {
            this.graphRef = r
            this.props.dygraphRef(r)
          }}
          className="dygraph-child-container"
          style={dygraphStyle}
        />
        {staticLegend && (
          <StaticLegend
            dygraphSeries={this.colorDygraphSeries}
            dygraph={this.dygraph}
            handleReceiveStaticLegendHeight={
              this.handleReceiveStaticLegendHeight
            }
          />
        )}
        {nestedGraph && React.cloneElement(nestedGraph, {staticLegendHeight})}
      </div>
    )
  }
}

const {array, arrayOf, bool, func, node, shape, string} = PropTypes

Dygraph.defaultProps = {
  axes: {
    y: {
      bounds: [null, null],
      prefix: '',
      suffix: '',
      base: BASE_10,
      scale: LINEAR,
    },
    y2: {
      bounds: undefined,
      prefix: '',
      suffix: '',
    },
  },
  containerStyle: {},
  isGraphFilled: true,
  overrideLineColors: null,
  dygraphRef: () => {},
  onZoom: () => {},
  handleSetHoverTime: () => {},
  staticLegend: {
    type: null,
  },
}

Dygraph.propTypes = {
  cellID: string,
  handleSetHoverTime: func,
  axes: shape({
    y: shape({
      bounds: array,
    }),
    y2: shape({
      bounds: array,
    }),
  }),
  queries: arrayOf(shape),
  timeSeries: array.isRequired,
  labels: array.isRequired,
  options: shape({}),
  containerStyle: shape({}),
  isGraphFilled: bool,
  isBarGraph: bool,
  staticLegend: bool,
  overrideLineColors: arrayOf(
    shape({
      type: string.isRequired,
      hex: string.isRequired,
    }).isRequired
  ),
  dygraphSeries: shape({}).isRequired,
  ruleValues: shape({
    operator: string,
    value: string,
    rangeValue: string,
  }),
  timeRange: shape({
    lower: string.isRequired,
  }),
  setResolution: func,
  dygraphRef: func,
  onZoom: func,
  mode: string,
  children: node,
  colors: colorsStringSchema.isRequired,
}

const mapStateToProps = ({annotations: {mode}}) => ({
  mode,
})

export default connect(mapStateToProps, null)(Dygraph)
