import _ from 'lodash'
import React, {Component} from 'react'
import PropTypes from 'prop-types'
import Dygraph from 'shared/components/Dygraph'

import SingleStat from 'src/shared/components/SingleStat'
import {timeSeriesToDygraph} from 'utils/timeSeriesTransformers'

import {colorsStringSchema} from 'shared/schemas'
import {ErrorHandlingWith} from 'src/shared/decorators/errors'
import InvalidData from 'src/shared/components/InvalidData'

const validateTimeSeries = timeseries => {
  return _.every(timeseries, r =>
    _.every(
      r,
      (v, i) => (i === 0 && Date.parse(v)) || _.isNumber(v) || _.isNull(v)
    )
  )
}

@ErrorHandlingWith(InvalidData)
class LineGraph extends Component {
  constructor(props) {
    super(props)
    this.isValidData = true
  }

  componentWillMount() {
    const {data, isInDataExplorer} = this.props
    this.parseTimeSeries(data, isInDataExplorer)
  }

  parseTimeSeries(data, isInDataExplorer) {
    this._timeSeries = timeSeriesToDygraph(data, isInDataExplorer)
    this.isValidData = validateTimeSeries(
      _.get(this._timeSeries, 'timeSeries', [])
    )
  }

  componentWillUpdate(nextProps) {
    const {data, activeQueryIndex} = this.props
    if (
      data !== nextProps.data ||
      activeQueryIndex !== nextProps.activeQueryIndex
    ) {
      this.parseTimeSeries(nextProps.data, nextProps.isInDataExplorer)
    }
  }

  render() {
    if (!this.isValidData) {
      return <InvalidData />
    }

    const {
      data,
      axes,
      title,
      colors,
      cellID,
      onZoom,
      queries,
      timeRange,
      ruleValues,
      isBarGraph,
      isRefreshing,
      setResolution,
      isGraphFilled,
      showSingleStat,
      displayOptions,
      staticLegend,
      underlayCallback,
      isFetchingInitially,
      handleSetHoverTime,
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
      fillGraph: true,
      underlayCallback,
      axisLabelWidth: 60,
      drawAxesAtZero: true,
      axisLineColor: '#383846',
      gridLineColor: '#383846',
      connectSeparatedPoints: true,
    }

    const containerStyle = {
      width: 'calc(100% - 32px)',
      height: 'calc(100% - 16px)',
      position: 'absolute',
      top: '8px',
    }

    const prefix = axes ? axes.y.prefix : ''
    const suffix = axes ? axes.y.suffix : ''

    return (
      <div className="dygraph graph--hasYLabel" style={{height: '100%'}}>
        {isRefreshing ? <GraphLoadingDots /> : null}
        <Dygraph
          axes={axes}
          cellID={cellID}
          colors={colors}
          onZoom={onZoom}
          labels={labels}
          queries={queries}
          options={options}
          timeRange={timeRange}
          isBarGraph={isBarGraph}
          timeSeries={timeSeries}
          ruleValues={ruleValues}
          dygraphSeries={dygraphSeries}
          setResolution={setResolution}
          handleSetHoverTime={handleSetHoverTime}
          containerStyle={containerStyle}
          staticLegend={staticLegend}
          isGraphFilled={showSingleStat ? false : isGraphFilled}
        >
          {showSingleStat && (
            <SingleStat
              prefix={prefix}
              suffix={suffix}
              data={data}
              lineGraph={true}
              colors={colors}
            />
          )}
        </Dygraph>
      </div>
    )
  }
}

const GraphLoadingDots = () => (
  <div className="graph-panel__refreshing">
    <div />
    <div />
    <div />
  </div>
)

const GraphSpinner = () => (
  <div className="graph-fetching">
    <div className="graph-spinner" />
  </div>
)

const {array, arrayOf, bool, func, number, shape, string} = PropTypes

LineGraph.defaultProps = {
  underlayCallback: () => {},
  isGraphFilled: true,
  staticLegend: false,
}

LineGraph.propTypes = {
  cellID: string,
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
  handleSetHoverTime: func,
  title: string,
  isFetchingInitially: bool,
  isRefreshing: bool,
  underlayCallback: func,
  isGraphFilled: bool,
  isBarGraph: bool,
  staticLegend: bool,
  showSingleStat: bool,
  displayOptions: shape({
    stepPlot: bool,
    stackedGraph: bool,
    animatedZooms: bool,
  }),
  activeQueryIndex: number,
  ruleValues: shape({}),
  timeRange: shape({
    lower: string.isRequired,
  }),
  isInDataExplorer: bool,
  setResolution: func,
  onZoom: func,
  queries: arrayOf(shape({}).isRequired).isRequired,
  data: arrayOf(shape({}).isRequired).isRequired,
  colors: colorsStringSchema,
}

export default LineGraph
