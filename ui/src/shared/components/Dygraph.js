/* eslint-disable no-magic-numbers */
import React, {PropTypes} from 'react'
import _ from 'lodash'
import Dygraph from '../../external/dygraph'
import getRange from 'src/shared/parsing/getRangeForDygraph'

const {array, arrayOf, number, bool, shape, string} = PropTypes

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

export default React.createClass({
  displayName: 'Dygraph',

  propTypes: {
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
    timeRange: shape({
      lower: string.isRequired,
    }),
  },

  getDefaultProps() {
    return {
      containerStyle: {},
      isGraphFilled: true,
      overrideLineColors: null,
      legendOnBottom: false,
    }
  },

  getTimeSeries() {
    // Avoid 'Can't plot empty data set' errors by falling back to a
    // default dataset that's valid for Dygraph.
    return this.props.timeSeries.length ? this.props.timeSeries : [[0]]
  },

  componentDidMount() {
    const timeSeries = this.getTimeSeries()
    // dygraphSeries is a legend label and its corresponding y-axis e.g. {legendLabel1: 'y', legendLabel2: 'y2'};
    const {ranges, dygraphSeries, ruleValues, legendOnBottom} = this.props

    const refs = this.refs
    const graphContainerNode = refs.graphContainer
    const legendContainerNode = refs.legendContainer
    const markerNode = refs.graphVerticalMarker
    let finalLineColors = this.props.overrideLineColors

    if (finalLineColors === null) {
      finalLineColors = LINE_COLORS
    }

    const defaultOptions = {
      labelsSeparateLines: false,
      labelsDiv: legendContainerNode,
      labelsKMB: true,
      rightGap: 0,
      leftGap: 0,
      highlightSeriesBackgroundAlpha: 1,
      fillGraph: this.props.isGraphFilled,
      axisLineWidth: 2,
      gridLineWidth: 1,
      highlightCircleSize: 3,
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
      highlightCallback(e, x, points) {
        // Move the Legend on hover
        const graphRect = graphContainerNode.getBoundingClientRect()
        const legendRect = legendContainerNode.getBoundingClientRect()
        const graphWidth = graphRect.width + 32 // Factoring in padding from parent
        const legendWidth = legendRect.width
        const legendMaxLeft = graphWidth - legendWidth / 2
        const trueGraphX = e.pageX - graphRect.left
        const legendTop = graphRect.height + 0
        let legendLeft = trueGraphX
        // Enforcing max & min legend offsets
        if (trueGraphX < legendWidth / 2) {
          legendLeft = legendWidth / 2
        } else if (trueGraphX > legendMaxLeft) {
          legendLeft = legendMaxLeft
        }

        legendContainerNode.style.left = `${legendLeft}px`
        if (legendOnBottom) {
          legendContainerNode.style.bottom = '4px'
        } else {
          legendContainerNode.style.top = `${legendTop}px`
        }

        setMarker(points)
      },
      unhighlightCallback() {
        removeMarker()
      },
    }

    const options = Object.assign({}, defaultOptions, this.props.options)

    this.dygraph = new Dygraph(graphContainerNode, timeSeries, options)

    function setMarker(points) {
      markerNode.style.left = `${points[0].canvasx}px`
      markerNode.style.display = 'block'
    }

    function removeMarker() {
      markerNode.style.display = 'none'
    }
  },

  componentWillUnmount() {
    this.dygraph.destroy()
    delete this.dygraph
  },

  // Only update the component if props other than timeRange have updateDashboard.
  // If only timeRange has changed, only reset the zoom.
  // TODO: possibly try to use dateWindow and a zoomCallback to updateOptions
  // in comonentDidUpdate so that zoom and redraw happen at the same time
  shouldComponentUpdate(nextProps) {
    const timeRangeChanged =
      nextProps.timeRange.lower !== this.props.timeRange.lower ||
      nextProps.timeRange.upper !== this.props.timeRange.upper

    if (this.dygraph.isZoomed() && timeRangeChanged) {
      this.dygraph.resetZoom()
    }

    const propsChanged = []

    // perform a deep diff on props objects to see what keys have changed
    // from https://stackoverflow.com/questions/8572826/generic-deep-diff-between-two-objects
    _.mergeWith(this.props, nextProps, (objectValue, sourceValue, key) => {
      if (
        !_.isEqual(objectValue, sourceValue) &&
        Object(objectValue) !== objectValue
      ) {
        propsChanged.push(key)
      }
    })

    const propsChangedWithoutTimeRange = _.without(
      propsChanged,
      'lower',
      'upper'
    )

    return propsChangedWithoutTimeRange.length
  },

  componentDidUpdate() {
    const dygraph = this.dygraph
    if (!dygraph) {
      throw new Error(
        'Dygraph not configured in time; this should not be possible!'
      )
    }

    const timeSeries = this.getTimeSeries()
    const {labels, ranges, options, dygraphSeries, ruleValues} = this.props

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

    dygraph.resize()
  },

  render() {
    return (
      <div ref="self" style={{height: '100%'}}>
        <div ref="graphContainer" style={this.props.containerStyle} />
        <div className="container--dygraph-legend" ref="legendContainer" />
        <div className="graph-vertical-marker" ref="graphVerticalMarker" />
      </div>
    )
  },
})
