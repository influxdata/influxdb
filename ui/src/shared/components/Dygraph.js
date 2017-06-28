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
      isSynced: false,
      legend: {
        x: null,
        series: [],
      },
      sortType: null,
      legendOrder: 'asc',
      filterText: '',
    }

    this.getTimeSeries = ::this.getTimeSeries
    this.sync = ::this.sync
    this.handleSortLegend = ::this.handleSortLegend
    this.handleLegendInputChange = ::this.handleLegendInputChange
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
    const {legend, legendOrder} = this.state
    const series = this.sortByType(legend.series, sortType)

    if (legendOrder === 'asc') {
      return this.setState({
        legend: {...legend, series: series.reverse()},
        sortType,
        legendOrder: 'desc',
      })
    }

    this.setState({
      legend: {...legend, series},
      sortType,
      legendOrder: 'asc',
    })
  }

  handleLegendInputChange(e) {
    this.setState({filterText: e.target.value})
  }

  sortByType(list, sortType, order) {
    if (!sortType) {
      return list
    }

    const orderFunc = ({y, label}) => (sortType === 'numeric' ? y : label)
    const orderedList = _.sortBy(list, orderFunc)

    if (order === 'desc') {
      return orderedList.reverse()
    }

    return orderedList
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

        const {state, sortByType} = dygraphComponent
        const {legendOrder, sortType} = state
        if (legend.x === state.legend.x) {
          return ''
        }

        const orderedSeries = sortByType(legend.series, sortType, legendOrder)
        dygraphComponent.setState({legend: {...legend, series: orderedSeries}})
        return ''
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
    const dygraphComponent = this // eslint-disable-line consistent-this

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
      legendFormatter: legend => {
        if (!legend.x) {
          return ''
        }

        const {state, sortByType} = dygraphComponent
        const {sortType, legendOrder} = state
        if (legend.x === state.legend.x) {
          return ''
        }

        const orderedSeries = sortByType(legend.series, sortType, legendOrder)
        dygraphComponent.setState({
          legend: {...legend, series: orderedSeries},
        })
        return ''
      },
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
    const {legend, filterText} = this.state

    return (
      <div className="dygraph-child">
        <DygraphLegend
          {...legend}
          onSort={this.handleSortLegend}
          onInputChange={this.handleLegendInputChange}
          filterText={filterText}
        />
        <div
          ref={r => {
            this.graphContainer = r
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
