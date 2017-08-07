/* eslint-disable no-magic-numbers */
import React, {Component, PropTypes} from 'react'
import shallowCompare from 'react-addons-shallow-compare'

import _ from 'lodash'

import Dygraphs from 'src/external/dygraph'
import getRange from 'shared/parsing/getRangeForDygraph'

import {LINE_COLORS, multiColumnBarPlotter} from 'src/shared/graphs/helpers'
import DygraphLegend from 'src/shared/components/DygraphLegend'

const hasherino = (str, len) =>
  str
    .split('')
    .map(char => char.charCodeAt(0))
    .reduce((hash, code) => hash + code, 0) % len

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
      isFilterVisible: false,
    }

    this.sync = ::this.sync
    this.getTimeSeries = ::this.getTimeSeries
    this.handleSortLegend = ::this.handleSortLegend
    this.handleLegendInputChange = ::this.handleLegendInputChange
    this.handleSnipLabel = ::this.handleSnipLabel
    this.handleHideLegend = ::this.handleHideLegend
    this.handleToggleFilter = ::this.handleToggleFilter
    this.visibility = ::this.visibility
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

    const graphRef = this.graphRef
    const legendRef = this.legendRef
    const finalLineColors = [...(overrideLineColors || LINE_COLORS)]

    const hashColorDygraphSeries = {}
    const {length} = finalLineColors

    for (const seriesName in dygraphSeries) {
      const series = dygraphSeries[seriesName]
      const hashIndex = hasherino(seriesName, length)
      const color = finalLineColors[hashIndex]
      hashColorDygraphSeries[seriesName] = {...series, color}
    }

    const defaultOptions = {
      plugins: isBarGraph
        ? []
        : [
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
      highlightCircleSize: isBarGraph ? 0 : 3,
      animatedZooms: true,
      hideOverlayOnMouseOut: false,
      colors: finalLineColors,
      series: hashColorDygraphSeries,
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
        highlightCircleSize: isBarGraph ? 0 : 5,
      },
      legendFormatter: legend => {
        if (!legend.x) {
          return ''
        }

        const {state: {legend: prevLegend}} = this
        const highlighted = legend.series.find(s => s.isHighlighted)
        const prevHighlighted = prevLegend.series.find(s => s.isHighlighted)

        const y = highlighted && highlighted.y
        const prevY = prevHighlighted && prevHighlighted.y

        if (legend.x === prevLegend.x && y === prevY) {
          return ''
        }

        this.setState({legend})
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

          if (!this.visibility().find(bool => bool === true)) {
            this.setState({filterText: ''})
          }
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

  visibility() {
    const timeSeries = this.getTimeSeries()
    const {filterText, legend} = this.state
    const series = _.get(timeSeries, '0', [])
    const numSeries = series.length
    return Array(numSeries ? numSeries - 1 : numSeries)
      .fill(true)
      .map((s, i) => {
        if (!legend.series[i]) {
          return true
        }

        return !!legend.series[i].label.match(filterText)
      })
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
    const updateOptions = {
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
      visibility: this.visibility(),
    }

    dygraph.updateOptions(updateOptions)
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

  handleSortLegend(sortType) {
    this.setState({sortType, isAscending: !this.state.isAscending})
  }

  handleLegendInputChange(e) {
    this.setState({filterText: e.target.value})
  }

  handleSnipLabel() {
    this.setState({isSnipped: !this.state.isSnipped})
  }

  handleToggleFilter() {
    this.setState({
      isFilterVisible: !this.state.isFilterVisible,
      filterText: '',
    })
  }

  handleHideLegend(e) {
    const {top, bottom, left, right} = this.graphRef.getBoundingClientRect()

    const mouseY = e.clientY
    const mouseX = e.clientX

    const mouseInGraphY = mouseY <= bottom && mouseY >= top
    const mouseInGraphX = mouseX <= right && mouseX >= left
    const isMouseHoveringGraph = mouseInGraphY && mouseInGraphX

    if (!isMouseHoveringGraph) {
      this.setState({isHidden: true})
      if (!this.visibility().find(bool => bool === true)) {
        this.setState({filterText: ''})
      }
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
      isFilterVisible,
    } = this.state

    return (
      <div className="dygraph-child">
        <DygraphLegend
          {...legend}
          sortType={sortType}
          onHide={this.handleHideLegend}
          isHidden={isHidden}
          isFilterVisible={isFilterVisible}
          isSnipped={isSnipped}
          filterText={filterText}
          isAscending={isAscending}
          onSnip={this.handleSnipLabel}
          onSort={this.handleSortLegend}
          legendRef={el => (this.legendRef = el)}
          onInputChange={this.handleLegendInputChange}
          onToggleFilter={this.handleToggleFilter}
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
