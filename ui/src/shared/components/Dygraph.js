/* eslint-disable no-magic-numbers */
import React, {Component, PropTypes} from 'react'
import shallowCompare from 'react-addons-shallow-compare'

import _ from 'lodash'

import Dygraphs from 'src/external/dygraph'
import getRange from 'shared/parsing/getRangeForDygraph'

import {LINE_COLORS, multiColumnBarPlotter} from 'src/shared/graphs/helpers'
import DygraphLegend from 'src/shared/components/DygraphLegend'

export default class Dygraph extends Component {
  constructor(props) {
    super(props)
    this.state = {
      legend: {
        x: null,
        series: [],
      },
      sortType: '',
      filterText: '',
      isSynced: false,
      isHidden: true,
      isAscending: true,
      isSnipped: false,
    }

    this.getTimeSeries = ::this.getTimeSeries
    this.sync = ::this.sync
    this.handleSortLegend = ::this.handleSortLegend
    this.handleLegendInputChange = ::this.handleLegendInputChange
    this.handleSnipLabel = ::this.handleSnipLabel
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

  handleSortLegend(sortType) {
    this.setState({sortType, isAscending: !this.state.isAscending})
  }

  handleLegendInputChange(e) {
    this.setState({filterText: e.target.value})
  }

  handleSnipLabel() {
    this.setState({isSnipped: !this.state.isSnipped})
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

    const graphRef = this.graphRef
    const legendRef = this.legendRef
    let finalLineColors = overrideLineColors

    if (finalLineColors === null) {
      finalLineColors = LINE_COLORS
    }

    const dygraphComponent = this // eslint-disable-line consistent-this

    const defaultOptions = {
      plugins: [
        new Dygraphs.Plugins.Crosshair({
          direction: 'vertical',
        }),
      ],
      labelsSeparateLines: false,
      labelsKMB: true,
      rightGap: 0,
      highlightSeriesBackgroundAlpha: 1.0,
      highlightSeriesBackgroundColor: 'rgb(41, 41, 51)',
      fillGraph: isGraphFilled,
      axisLineWidth: 2,
      gridLineWidth: 1,
      highlightCircleSize: 3,
      animatedZooms: true,
      hideOverlayOnMouseOut: false,
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
      legendFormatter: legend => {
        if (!legend.x) {
          return ''
        }

        const oldState = dygraphComponent.state

        const newHighlighted = legend.series.find(s => s.isHighlighted)
        const highlighted = oldState.legend.series.find(s => s.isHighlighted)

        const isSame =
          legend.x === oldState.legend.x && newHighlighted.y === highlighted.y

        if (isSame) {
          return ''
        }

        dygraphComponent.setState({legend})
        return ''
      },
      highlightCallback: e => {
        // Move the Legend on hover
        const graphRect = graphRef.getBoundingClientRect()
        const legendRect = legendRef.getBoundingClientRect()

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

        legendRef.style.left = `${legendLeft}px`
        legendRef.style.top = `${legendTop}px`

        this.setState({isHidden: false})
      },
      unhighlightCallback: e => {
        const {top, bottom, left, right} = legendRef.getBoundingClientRect()

        const mouseY = e.clientY
        const mouseX = e.clientX

        const mouseInLegendY = mouseY <= bottom && mouseY >= top
        const mouseInLegendX = mouseX <= right && mouseX >= left
        const isMouseHoveringLegend = mouseInLegendY && mouseInLegendX

        if (!isMouseHoveringLegend) {
          this.setState({isHidden: true})
        }
      },
    }

    if (isBarGraph) {
      defaultOptions.plotter = multiColumnBarPlotter
    }

    this.dygraph = new Dygraphs(graphRef, timeSeries, {
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
    const {
      legend,
      filterText,
      isAscending,
      sortType,
      isHidden,
      isSnipped,
    } = this.state

    return (
      <div className="dygraph-child">
        <DygraphLegend
          {...legend}
          sortType={sortType}
          isHidden={isHidden}
          isSnipped={isSnipped}
          filterText={filterText}
          isAscending={isAscending}
          onSnip={this.handleSnipLabel}
          onSort={this.handleSortLegend}
          legendRef={el => this.legendRef = el}
          onInputChange={this.handleLegendInputChange}
        />
        <div
          ref={r => {
            this.graphRef = r
          }}
          style={this.props.containerStyle}
          className="dygraph-child-container"
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
