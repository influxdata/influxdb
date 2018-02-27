import React, {PropTypes} from 'react'

import {emptyGraphCopy} from 'src/shared/copy/cell'

import AutoRefresh from 'shared/components/AutoRefresh'
import LineGraph from 'shared/components/LineGraph'
import SingleStat from 'shared/components/SingleStat'
import GaugeChart from 'shared/components/GaugeChart'

const RefreshingLineGraph = AutoRefresh(LineGraph)
const RefreshingSingleStat = AutoRefresh(SingleStat)
const RefreshingGaugeChart = AutoRefresh(GaugeChart)

const RefreshingGraph = ({
  axes,
  inView,
  type,
  colors,
  onZoom,
  cellID,
  queries,
  templates,
  timeRange,
  cellHeight,
  autoRefresh,
  resizerTopHeight,
  manualRefresh, // when changed, re-mounts the component
  synchronizer,
  resizeCoords,
  editQueryStatus,
  grabDataForDownload,
}) => {
  const prefix = (axes && axes.y.prefix) || ''
  const suffix = (axes && axes.y.suffix) || ''

  if (!queries.length) {
    return (
      <div className="graph-empty">
        <p data-test="data-explorer-no-results">
          {emptyGraphCopy}
        </p>
      </div>
    )
  }

  if (type === 'single-stat') {
    return (
      <RefreshingSingleStat
        colors={colors}
        key={manualRefresh}
        queries={[queries[0]]}
        templates={templates}
        autoRefresh={autoRefresh}
        cellHeight={cellHeight}
        prefix={prefix}
        suffix={suffix}
        inView={inView}
      />
    )
  }

  if (type === 'gauge') {
    return (
      <RefreshingGaugeChart
        colors={colors}
        key={manualRefresh}
        queries={[queries[0]]}
        templates={templates}
        autoRefresh={autoRefresh}
        cellHeight={cellHeight}
        resizerTopHeight={resizerTopHeight}
        resizeCoords={resizeCoords}
        cellID={cellID}
        prefix={prefix}
        suffix={suffix}
        inView={inView}
      />
    )
  }

  const displayOptions = {
    stepPlot: type === 'line-stepplot',
    stackedGraph: type === 'line-stacked',
  }

  return (
    <RefreshingLineGraph
      axes={axes}
      colors={colors}
      onZoom={onZoom}
      queries={queries}
      inView={inView}
      key={manualRefresh}
      templates={templates}
      timeRange={timeRange}
      autoRefresh={autoRefresh}
      isBarGraph={type === 'bar'}
      synchronizer={synchronizer}
      resizeCoords={resizeCoords}
      displayOptions={displayOptions}
      editQueryStatus={editQueryStatus}
      grabDataForDownload={grabDataForDownload}
      showSingleStat={type === 'line-plus-single-stat'}
    />
  )
}

const {arrayOf, bool, func, number, shape, string} = PropTypes

RefreshingGraph.propTypes = {
  timeRange: shape({
    lower: string.isRequired,
  }),
  autoRefresh: number.isRequired,
  manualRefresh: number,
  templates: arrayOf(shape()),
  synchronizer: func,
  type: string.isRequired,
  cellHeight: number,
  resizerTopHeight: number,
  axes: shape(),
  queries: arrayOf(shape()).isRequired,
  editQueryStatus: func,
  onZoom: func,
  resizeCoords: shape(),
  grabDataForDownload: func,
  colors: arrayOf(
    shape({
      type: string.isRequired,
      hex: string.isRequired,
      id: string.isRequired,
      name: string.isRequired,
      value: string.isRequired,
    }).isRequired
  ),
  cellID: string,
  inView: bool,
}

RefreshingGraph.defaultProps = {
  manualRefresh: 0,
  inView: true,
}

export default RefreshingGraph
