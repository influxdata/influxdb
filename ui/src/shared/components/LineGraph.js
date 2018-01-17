import React, {PropTypes, Component} from 'react'
import Dygraph from 'shared/components/Dygraph'
import shallowCompare from 'react-addons-shallow-compare'

import SingleStat from 'src/shared/components/SingleStat'
import timeSeriesToDygraph from 'utils/timeSeriesToDygraph'

import {SINGLE_STAT_LINE_COLORS} from 'src/shared/graphs/helpers'

class LineGraph extends Component {
  constructor(props) {
    super(props)
  }

  shouldComponentUpdate(nextProps, nextState) {
    return shallowCompare(this, nextProps, nextState)
  }

  componentWillMount() {
    const {data, isInDataExplorer} = this.props
    this._timeSeries = timeSeriesToDygraph(data, isInDataExplorer)
  }

  componentWillUpdate(nextProps) {
    const {data, activeQueryIndex} = this.props
    if (
      data !== nextProps.data ||
      activeQueryIndex !== nextProps.activeQueryIndex
    ) {
      this._timeSeries = timeSeriesToDygraph(
        nextProps.data,
        nextProps.isInDataExplorer
      )
    }
  }

  render() {
    const {
      data,
      axes,
      cell,
      title,
      onZoom,
      queries,
      timeRange,
      cellHeight,
      ruleValues,
      isBarGraph,
      resizeCoords,
      synchronizer,
      isRefreshing,
      isGraphFilled,
      showSingleStat,
      displayOptions,
      underlayCallback,
      overrideLineColors,
      isFetchingInitially,
    } = this.props

    const {labels, timeSeries, dygraphSeries} = this._timeSeries

    // If data for this graph is being fetched for the first time, show a graph-wide spinner.
    if (isFetchingInitially) {
      return <GraphSpinner />
    }

    const options = {
      ...displayOptions,
      title,
      labels,
      rightGap: 0,
      yRangePad: 10,
      labelsKMB: true,
      underlayCallback,
      axisLabelWidth: 60,
      drawAxesAtZero: true,
      axisLineColor: '#383846',
      gridLineColor: '#383846',
      connectSeparatedPoints: true,
    }

    const lineColors = showSingleStat
      ? SINGLE_STAT_LINE_COLORS
      : overrideLineColors

    const containerStyle = {
      width: 'calc(100% - 32px)',
      height: 'calc(100% - 16px)',
      position: 'absolute',
      top: '8px',
    }

    return (
      <div className="dygraph graph--hasYLabel" style={{height: '100%'}}>
        {isRefreshing ? <GraphLoadingDots /> : null}
        <Dygraph
          cell={cell}
          axes={axes}
          onZoom={onZoom}
          labels={labels}
          queries={queries}
          timeRange={timeRange}
          isBarGraph={isBarGraph}
          timeSeries={timeSeries}
          ruleValues={ruleValues}
          synchronizer={synchronizer}
          resizeCoords={resizeCoords}
          overrideLineColors={lineColors}
          dygraphSeries={dygraphSeries}
          setResolution={this.props.setResolution}
          containerStyle={containerStyle}
          isGraphFilled={showSingleStat ? false : isGraphFilled}
          options={options}
        />
        {showSingleStat
          ? <SingleStat data={data} cellHeight={cellHeight} />
          : null}
      </div>
    )
  }
}

const GraphLoadingDots = () =>
  <div className="graph-panel__refreshing">
    <div />
    <div />
    <div />
  </div>

const GraphSpinner = () =>
  <div className="graph-fetching">
    <div className="graph-spinner" />
  </div>

const {array, arrayOf, bool, func, number, shape, string} = PropTypes

LineGraph.defaultProps = {
  underlayCallback: () => {},
  isGraphFilled: true,
  overrideLineColors: null,
}

LineGraph.propTypes = {
  axes: shape({
    y: shape({
      bounds: array,
      label: string,
    }),
    y2: shape({
      bounds: array,
      label: string,
    }),
  }),
  title: string,
  isFetchingInitially: bool,
  isRefreshing: bool,
  underlayCallback: func,
  isGraphFilled: bool,
  isBarGraph: bool,
  overrideLineColors: array,
  showSingleStat: bool,
  displayOptions: shape({
    stepPlot: bool,
    stackedGraph: bool,
  }),
  activeQueryIndex: number,
  ruleValues: shape({}),
  timeRange: shape({
    lower: string.isRequired,
  }),
  isInDataExplorer: bool,
  synchronizer: func,
  setResolution: func,
  cellHeight: number,
  cell: shape(),
  onZoom: func,
  resizeCoords: shape(),
  queries: arrayOf(shape({}).isRequired).isRequired,
  data: arrayOf(shape({}).isRequired).isRequired,
}

export default LineGraph
