/* eslint-disable no-magic-numbers */
import React, {Component, PropTypes} from 'react'
import Dygraphs from 'src/external/dygraph'
import getRange from 'src/shared/parsing/getRangeForDygraph'

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

export default class Dygraph extends Component {
  constructor(props) {
    super(props)
    this.state = {
      synced: false,
    }

    // workaround for dygraph.updateOptions breaking legends
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
    legendOnBottom: false,
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

        // this.isMouseOverGraph = false
      },
      highlightCallback: e => {
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
        const legendBottomExceedsScreen =
          graphBottom + legendHeight > screenHeight

        const legendTop = legendBottomExceedsScreen
          ? graphHeight + 8 - legendHeight
          : graphHeight + 8

        legendContainerNode.style.left = `${legendLeft}px`
        legendContainerNode.style.top = `${legendTop}px`
        legendContainerNode.className = 'container--dygraph-legend' // show

        // this.isMouseOverGraph = true
        // this.lastMouseMoveEvent = e
      },
      drawCallback: () => {
        legendContainerNode.className = 'container--dygraph-legend hidden' // hide
      },
    }

    this.dygraph = new Dygraphs(graphContainerNode, timeSeries, {
      ...defaultOptions,
      ...options,
    })

    this.sync()
  }

  componentWillUnmount() {
    this.dygraph.destroy()
    delete this.dygraph
  }

  componentDidUpdate() {
    const {labels, ranges, options, dygraphSeries, ruleValues} = this.props
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
    })
    // if (this.lastMouseMoveEvent) {
    //   dygraph.mouseMove_(this.lastMouseMoveEvent)
    // }

    dygraph.resize()
  }

  sync() {
    if (this.props.synchronizer && !this.state.synced) {
      this.props.synchronizer(this.dygraph)
      this.setState({synced: true})
    }
  }

  render() {
    return (
      <div style={{height: '100%'}}>
        <div
          ref={r => {
            this.graphContainer = r
          }}
          style={this.props.containerStyle}
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
  overrideLineColors: array,
  dygraphSeries: shape({}).isRequired,
  ruleValues: shape({
    operator: string,
    value: string,
    rangeValue: string,
  }),
  legendOnBottom: bool,
  synchronizer: func,
}
