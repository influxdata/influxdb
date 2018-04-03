import React from 'react'
import PropTypes from 'prop-types'

import {emptyGraphCopy} from 'src/shared/copy/cell'

import AutoRefresh from 'shared/components/AutoRefresh'
import LineGraph from 'shared/components/LineGraph'
import SingleStat from 'shared/components/SingleStat'
import GaugeChart from 'shared/components/GaugeChart'
import TableGraph from 'shared/components/TableGraph'

import {colorsStringSchema} from 'shared/schemas'

const RefreshingLineGraph = AutoRefresh(LineGraph)
const RefreshingSingleStat = AutoRefresh(SingleStat)
const RefreshingGaugeChart = AutoRefresh(GaugeChart)
const RefreshingTableGraph = AutoRefresh(TableGraph)

const RefreshingGraph = ({
  axes,
  inView,
  type,
  colors,
  onZoom,
  cellID,
  queries,
  tableOptions,
  setDataLabels,
  templates,
  timeRange,
  cellHeight,
  autoRefresh,
  resizerTopHeight,
  staticLegend,
  manualRefresh, // when changed, re-mounts the component
  resizeCoords,
  editQueryStatus,
  grabDataForDownload,
  hoverTime,
  onSetHoverTime,
}) => {
  const prefix = (axes && axes.y.prefix) || ''
  const suffix = (axes && axes.y.suffix) || ''

  if (!queries.length) {
    return (
      <div className="graph-empty">
        <p data-test="data-explorer-no-results">{emptyGraphCopy}</p>
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

  if (type === 'table') {
    return (
      <RefreshingTableGraph
        colors={colors}
        key={manualRefresh}
        queries={queries}
        templates={templates}
        autoRefresh={autoRefresh}
        cellHeight={cellHeight}
        resizerTopHeight={resizerTopHeight}
        resizeCoords={resizeCoords}
        cellID={cellID}
        tableOptions={tableOptions}
        hoverTime={hoverTime}
        onSetHoverTime={onSetHoverTime}
        inView={inView}
        setDataLabels={setDataLabels}
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
      hoverTime={hoverTime}
      onSetHoverTime={onSetHoverTime}
      resizeCoords={resizeCoords}
      staticLegend={staticLegend}
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
  hoverTime: string,
  onSetHoverTime: func,
  type: string.isRequired,
  cellHeight: number,
  resizerTopHeight: number,
  axes: shape(),
  queries: arrayOf(shape()).isRequired,
  editQueryStatus: func,
  staticLegend: bool,
  onZoom: func,
  resizeCoords: shape(),
  grabDataForDownload: func,
  colors: colorsStringSchema,
  cellID: string,
  inView: bool,
  tableOptions: shape({}),
  setDataLabels: func,
}

RefreshingGraph.defaultProps = {
  manualRefresh: 0,
  staticLegend: false,
  inView: true,
}

export default RefreshingGraph
