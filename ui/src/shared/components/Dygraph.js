/* eslint-disable no-magic-numbers */
import React, {Component, PropTypes} from 'react'
import shallowCompare from 'react-addons-shallow-compare'

import _ from 'lodash'

import Dygraphs from 'src/external/dygraph'
import getRange from 'shared/parsing/getRangeForDygraph'

const LINE_COLORS = [
  '#00C9FF',
  '#9394FF',
  '#4ED8A0',
  '#ff0054',
  '#ffcc00',
  '#33aa99',
  '#9dfc5d',
  '#92bcc3',
  '#ca96fb',
  '#ff00f0',
  '#38b94a',
  '#3844b9',
  '#a0725b',
]

const darkenColor = colorStr => {
  // Defined in dygraph-utils.js
  const color = Dygraphs.toRGB_(colorStr)
  color.r = Math.floor((255 + color.r) / 2)
  color.g = Math.floor((255 + color.g) / 2)
  color.b = Math.floor((255 + color.b) / 2)
  return `rgb(${color.r},${color.g},${color.b})`
}
// Bar Graph code below is from http://dygraphs.com/tests/plotters.html
const multiColumnBarPlotter = e => {
  // We need to handle all the series simultaneously.
  if (e.seriesIndex !== 0) {
    return
  }

  const g = e.dygraph
  const ctx = e.drawingContext
  const sets = e.allSeriesPoints
  const yBottom = e.dygraph.toDomYCoord(0)

  // Find the minimum separation between x-values.
  // This determines the bar width.
  let minSep = Infinity
  for (let j = 0; j < sets.length; j++) {
    const points = sets[j]
    for (let i = 1; i < points.length; i++) {
      const sep = points[i].canvasx - points[i - 1].canvasx
      if (sep < minSep) {
        minSep = sep
      }
    }
  }

  const barWidth = Math.floor(2.0 / 3 * minSep)

  const fillColors = []
  const strokeColors = g.getColors()
  for (let i = 0; i < strokeColors.length; i++) {
    fillColors.push(darkenColor(strokeColors[i]))
  }

  for (let j = 0; j < sets.length; j++) {
    ctx.fillStyle = fillColors[j]
    ctx.strokeStyle = strokeColors[j]
    for (let i = 0; i < sets[j].length; i++) {
      const p = sets[j][i]
      const centerX = p.canvasx
      const xLeft = sets.length === 1
        ? centerX - barWidth / 2
        : centerX - barWidth / 2 * (1 - j / (sets.length - 1))

      ctx.fillRect(
        xLeft,
        p.canvasy,
        barWidth / sets.length,
        yBottom - p.canvasy
      )

      ctx.strokeRect(
        xLeft,
        p.canvasy,
        barWidth / sets.length,
        yBottom - p.canvasy
      )
    }
  }
}

export default class Dygraph extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isSynced: false,
    }

    // optional workaround for dygraph.updateOptions breaking legends
    // a la http://stackoverflow.com/questions/38371876/dygraph-dynamic-update-legend-values-disappear
    // this.lastMouseMoveEvent = null
    // this.isMouseOverGraph = false

    this.getTimeSeries = ::this.getTimeSeries
    this.sync = ::this.sync
  }

  static defaultProps = {
    containerStyle: {},
    isGraphFilled: true,
    overrideLineColors: null,
  }

  getTimeSeries() {
    const {timeSeries} = this.props
    // Avoid 'Can't plot empty data set' errors by falling back to a
    // default dataset that's valid for Dygraph.
    return timeSeries.length ? timeSeries : [[0]]
  }

  componentDidMount() {
    const timeSeries = this.getTimeSeries()
    // dygraphSeries is a legend label and its corresponding y-axis e.g. {legendLabel1: 'y', legendLabel2: 'y2'};
    const {
      ranges,
      dygraphSeries,
      ruleValues,
      overrideLineColors,
      isGraphFilled,
      isBarGraph,
      options,
    } = this.props

    const graphContainerNode = this.graphContainer
    const legendContainerNode = this.legendContainer
    let finalLineColors = overrideLineColors

    if (finalLineColors === null) {
      finalLineColors = LINE_COLORS
    }

    const defaultOptions = {
      plugins: [
        new Dygraphs.Plugins.Crosshair({
          direction: 'vertical',
        }),
      ],
      labelsSeparateLines: false,
      labelsDiv: legendContainerNode,
      labelsKMB: true,
      rightGap: 0,
      highlightSeriesBackgroundAlpha: 1.0,
      highlightSeriesBackgroundColor: 'rgb(41, 41, 51)',
      fillGraph: isGraphFilled,
      axisLineWidth: 2,
      gridLineWidth: 1,
      highlightCircleSize: 3,
      animatedZooms: true,
      colors: finalLineColors,
      series: dygraphSeries,
      axes: {
        y: {
          valueRange: getRange(timeSeries, ranges.y, ruleValues),
        },
        y2: {
          valueRange: getRange(timeSeries, ranges.y2),
        },
      },
      highlightSeriesOpts: {
        strokeWidth: 2,
        highlightCircleSize: 5,
      },
      unhighlightCallback: () => {
        legendContainerNode.className = 'container--dygraph-legend hidden' // hide

        // part of optional workaround for preventing updateOptions from breaking legend
        // this.isMouseOverGraph = false
      },
      highlightCallback: e => {
        // don't make visible yet, but render on DOM to capture position for calcs
        legendContainerNode.style.visibility = 'hidden'
        legendContainerNode.className = 'container--dygraph-legend'

        // Move the Legend on hover
        const graphRect = graphContainerNode.getBoundingClientRect()
        const legendRect = legendContainerNode.getBoundingClientRect()
        const graphWidth = graphRect.width + 32 // Factoring in padding from parent
        const graphHeight = graphRect.height
        const graphBottom = graphRect.bottom
        const legendWidth = legendRect.width
        const legendHeight = legendRect.height
        const screenHeight = window.innerHeight
        const legendMaxLeft = graphWidth - legendWidth / 2
        const trueGraphX = e.pageX - graphRect.left

        let legendLeft = trueGraphX

        // Enforcing max & min legend offsets
        if (trueGraphX < legendWidth / 2) {
          legendLeft = legendWidth / 2
        } else if (trueGraphX > legendMaxLeft) {
          legendLeft = legendMaxLeft
        }

        // Disallow screen overflow of legend
        const isLegendBottomClipped = graphBottom + legendHeight > screenHeight

        const legendTop = isLegendBottomClipped
          ? graphHeight + 8 - legendHeight
          : graphHeight + 8

        legendContainerNode.style.visibility = 'visible' // show
        legendContainerNode.style.left = `${legendLeft}px`
        legendContainerNode.style.top = `${legendTop}px`

        // part of optional workaround for preventing updateOptions from breaking legend
        // this.isMouseOverGraph = true
        // this.lastMouseMoveEvent = e
      },
      drawCallback: () => {
        legendContainerNode.className = 'container--dygraph-legend hidden' // hide
      },
    }

    if (isBarGraph) {
      defaultOptions.plotter = multiColumnBarPlotter
    }

    this.dygraph = new Dygraphs(graphContainerNode, timeSeries, {
      ...defaultOptions,
      ...options,
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
    const {
      labels,
      ranges,
      options,
      dygraphSeries,
      ruleValues,
      isBarGraph,
    } = this.props
    const dygraph = this.dygraph
    if (!dygraph) {
      throw new Error(
        'Dygraph not configured in time; this should not be possible!'
      )
    }

    const timeSeries = this.getTimeSeries()

    const legendContainerNode = this.legendContainer
    legendContainerNode.className = 'container--dygraph-legend hidden' // hide

    dygraph.updateOptions({
      labels,
      file: timeSeries,
      axes: {
        y: {
          valueRange: getRange(timeSeries, ranges.y, ruleValues),
        },
        y2: {
          valueRange: getRange(timeSeries, ranges.y2),
        },
      },
      stepPlot: options.stepPlot,
      stackedGraph: options.stackedGraph,
      underlayCallback: options.underlayCallback,
      series: dygraphSeries,
      plotter: isBarGraph ? multiColumnBarPlotter : null,
    })
    // part of optional workaround for preventing updateOptions from breaking legend
    // if (this.lastMouseMoveEvent) {
    //   dygraph.mouseMove_(this.lastMouseMoveEvent)
    // }

    dygraph.resize()
    const {w} = this.dygraph.getArea()
    this.props.setResolution(w)
  }

  sync() {
    if (!this.state.isSynced) {
      this.props.synchronizer(this.dygraph)
      this.setState({isSynced: true})
    }
  }

  render() {
    return (
      <div className="dygraph-child">
        <div
          ref={r => {
            this.graphContainer = r
          }}
          style={this.props.containerStyle}
          className="dygraph-child-container"
        />
        <div
          ref={r => {
            this.legendContainer = r
          }}
          className={'container--dygraph-legend hidden'}
        />
      </div>
    )
  }
}

const {array, arrayOf, func, number, bool, shape, string} = PropTypes

Dygraph.propTypes = {
  ranges: shape({
    y: arrayOf(number),
    y2: arrayOf(number),
  }),
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
}
