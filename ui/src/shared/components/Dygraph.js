/* eslint-disable no-magic-numbers */
import React, {Component, PropTypes} from 'react'
import shallowCompare from 'react-addons-shallow-compare'
import _ from 'lodash'
import NanoDate from 'nano-date'

import Dygraphs from 'src/external/dygraph'
import DygraphLegend from 'src/shared/components/DygraphLegend'
import Annotations from 'src/shared/components/Annotations'

import getRange, {getStackedRange} from 'shared/parsing/getRangeForDygraph'
import {DISPLAY_OPTIONS} from 'src/dashboards/constants'
import {buildDefaultYLabel} from 'shared/presenters'
import {numberValueFormatter} from 'src/utils/formatting'
import {getAnnotations} from 'src/shared/annotations/helpers'

import {
  OPTIONS,
  LINE_COLORS,
  LABEL_WIDTH,
  CHAR_PIXELS,
  barPlotter,
  hasherino,
  highlightSeriesOpts,
} from 'src/shared/graphs/helpers'
const {LINEAR, LOG, BASE_10, BASE_2} = DISPLAY_OPTIONS

export default class Dygraph extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isSynced: false,
      isHidden: true,
    }
  }

  componentDidMount() {
    const {
      axes: {y, y2},
      isGraphFilled: fillGraph,
      isBarGraph,
      options,
    } = this.props

    const timeSeries = this.getTimeSeries()
    const graphRef = this.graphRef

    let defaultOptions = {
      fillGraph,
      logscale: y.scale === LOG,
      colors: this.getLineColors(),
      series: this.hashColorDygraphSeries(),
      highlightCallback: this.highlightCallback,
      unhighlightCallback: this.unhighlightCallback,
      plugins: [new Dygraphs.Plugins.Crosshair({direction: 'vertical'})],
      axes: {
        y: {
          valueRange: this.getYRange(timeSeries),
          axisLabelFormatter: (yval, __, opts) =>
            numberValueFormatter(yval, opts, y.prefix, y.suffix),
          axisLabelWidth: this.getLabelWidth(),
          labelsKMB: y.base === BASE_10,
          labelsKMG2: y.base === BASE_2,
        },
        y2: {
          valueRange: getRange(timeSeries, y2.bounds),
        },
      },
      highlightSeriesOpts,
      zoomCallback: (lower, upper) => this.handleZoom(lower, upper),
    }

    if (isBarGraph) {
      defaultOptions = {
        ...defaultOptions,
        plotter: barPlotter,
        plugins: [],
        highlightSeriesOpts: {
          ...highlightSeriesOpts,
          highlightCircleSize: 0,
        },
      }
    }

    this.dygraph = new Dygraphs(graphRef, timeSeries, {
      ...defaultOptions,
      ...options,
      ...OPTIONS,
    })

    const {w} = this.dygraph.getArea()
    this.props.setResolution(w)

    // Simple opt-out for now, if a graph should not be synced
    if (this.props.synchronizer) {
      this.sync()
    }
  }

  componentWillUnmount() {
    this.dygraph.destroy()
    delete this.dygraph
  }

  shouldComponentUpdate(nextProps, nextState) {
    const timeRangeChanged = !_.isEqual(
      nextProps.timeRange,
      this.props.timeRange
    )

    if (this.dygraph.isZoomed() && timeRangeChanged) {
      this.dygraph.resetZoom()
    }

    // Will cause componentDidUpdate to fire twice, currently. This could
    // be reduced by returning false from within the reset conditional above,
    // though that would be based on the assumption that props for timeRange
    // will always change before those for data.
    return shallowCompare(this, nextProps, nextState)
  }

  componentDidUpdate() {
    const {labels, axes: {y, y2}, options, isBarGraph} = this.props

    const dygraph = this.dygraph
    if (!dygraph) {
      throw new Error(
        'Dygraph not configured in time; this should not be possible!'
      )
    }

    const timeSeries = this.getTimeSeries()

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
          axisLabelWidth: this.getLabelWidth(),
          labelsKMB: y.base === BASE_10,
          labelsKMG2: y.base === BASE_2,
        },
        y2: {
          valueRange: getRange(timeSeries, y2.bounds),
        },
      },
      colors: this.getLineColors(),
      series: this.hashColorDygraphSeries(),
      plotter: isBarGraph ? barPlotter : null,
      legendFormatter: this.legendComponent.legendFormatter,
      drawCallback: graph => {
        this.annotationsRef.setState({dygraph: graph})
      },
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

  hashColorDygraphSeries = () => {
    const {dygraphSeries} = this.props
    const colors = this.getLineColors()
    const hashColorDygraphSeries = {}

    for (const seriesName in dygraphSeries) {
      const series = dygraphSeries[seriesName]
      const hashIndex = hasherino(seriesName, colors.length)
      const color = colors[hashIndex]
      hashColorDygraphSeries[seriesName] = {...series, color}
    }

    return hashColorDygraphSeries
  }

  sync = () => {
    if (!this.state.isSynced) {
      this.props.synchronizer(this.dygraph)
      this.setState({isSynced: true})
    }
  }

  handleHideLegend = e => {
    const {top, bottom, left, right} = this.graphRef.getBoundingClientRect()

    const mouseY = e.clientY
    const mouseX = e.clientX

    const mouseInGraphY = mouseY <= bottom && mouseY >= top
    const mouseInGraphX = mouseX <= right && mouseX >= left
    const isMouseHoveringGraph = mouseInGraphY && mouseInGraphX

    if (!isMouseHoveringGraph) {
      this.setState({isHidden: true})
    }
  }

  getLineColors = () => {
    return [...(this.props.overrideLineColors || LINE_COLORS)]
  }

  getLabelWidth = () => {
    const {axes: {y}} = this.props
    return (
      LABEL_WIDTH +
      y.prefix.length * CHAR_PIXELS +
      y.suffix.length * CHAR_PIXELS
    )
  }

  getTimeSeries = () => {
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

  handleLegendRef = el => (this.legendNodeRef = el)

  handleLegendComponentRef = ref => (this.legendComponent = ref)

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

  deselectCrosshair = () => {
    const plugins = this.dygraph.plugins_
    const crosshair = plugins.find(
      ({plugin}) => plugin.toString() === 'Crosshair Plugin'
    )

    if (!crosshair || this.props.isBarGraph) {
      return
    }

    crosshair.plugin.deselect()
  }

  unhighlightCallback = e => {
    const {
      top,
      bottom,
      left,
      right,
    } = this.legendNodeRef.getBoundingClientRect()

    const mouseY = e.clientY
    const mouseX = e.clientX

    const mouseBuffer = 5
    const mouseInLegendY = mouseY <= bottom && mouseY >= top - mouseBuffer
    const mouseInLegendX = mouseX <= right && mouseX >= left
    const isMouseHoveringLegend = mouseInLegendY && mouseInLegendX

    if (!isMouseHoveringLegend) {
      this.setState({isHidden: true})
    }
  }

  highlightCallback = ({pageX}) => {
    this.legendComponent.setState({pageX})
    this.setState({isHidden: false})
  }

  handleAnnotationsRef = ref => (this.annotationsRef = ref)

  render() {
    const {isHidden} = this.state
    const {annotations} = this.props

    return (
      <div className="dygraph-child" onMouseLeave={this.deselectCrosshair}>
        <Annotations
          annotationsRef={this.handleAnnotationsRef}
          annotations={getAnnotations(this.dygraph, annotations)}
        />
        <DygraphLegend
          dygraph={this.dygraph}
          graph={this.graphRef}
          isHidden={isHidden}
          legendNode={this.legendNodeRef}
          onHide={this.handleHideLegend}
          legendNodeRef={this.handleLegendRef}
          legendComponent={this.handleLegendComponentRef}
        />
        <div
          ref={r => {
            this.graphRef = r
            this.props.dygraphRef(r)
          }}
          className="dygraph-child-container"
          style={{...this.props.containerStyle, zIndex: '2'}}
        />
      </div>
    )
  }
}

const {array, arrayOf, bool, func, shape, string} = PropTypes

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
}

Dygraph.propTypes = {
  annotations: arrayOf(shape({})),
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
  overrideLineColors: array,
  dygraphSeries: shape({}).isRequired,
  ruleValues: shape({
    operator: string,
    value: string,
    rangeValue: string,
  }),
  timeRange: shape({
    lower: string.isRequired,
  }),
  synchronizer: func,
  setResolution: func,
  dygraphRef: func,
  onZoom: func,
}
