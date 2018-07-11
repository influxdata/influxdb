import React from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'

import {emptyGraphCopy} from 'src/shared/copy/cell'
import {bindActionCreators} from 'redux'

import AutoRefresh from 'shared/components/AutoRefresh'
import LineGraph from 'shared/components/LineGraph'
import SingleStat from 'shared/components/SingleStat'
import GaugeChart from 'shared/components/GaugeChart'
import TableGraph from 'shared/components/TableGraph'

import {colorsStringSchema} from 'shared/schemas'
import {setHoverTime} from 'src/dashboards/actions'
import {
  DEFAULT_TIME_FORMAT,
  DEFAULT_DECIMAL_PLACES,
} from 'src/dashboards/constants'

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
  source,
  templates,
  timeRange,
  cellHeight,
  autoRefresh,
  fieldOptions,
  timeFormat,
  tableOptions,
  decimalPlaces,
  onSetResolution,
  resizerTopHeight,
  staticLegend,
  manualRefresh, // when changed, re-mounts the component
  editQueryStatus,
  handleSetHoverTime,
  grabDataForDownload,
  isInCEO,
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
        type={type}
        source={source}
        colors={colors}
        prefix={prefix}
        suffix={suffix}
        inView={inView}
        key={manualRefresh}
        templates={templates}
        queries={[queries[0]]}
        cellHeight={cellHeight}
        autoRefresh={autoRefresh}
        decimalPlaces={decimalPlaces}
        editQueryStatus={editQueryStatus}
        onSetResolution={onSetResolution}
      />
    )
  }

  if (type === 'gauge') {
    return (
      <RefreshingGaugeChart
        type={type}
        source={source}
        cellID={cellID}
        prefix={prefix}
        suffix={suffix}
        inView={inView}
        colors={colors}
        key={manualRefresh}
        queries={[queries[0]]}
        templates={templates}
        autoRefresh={autoRefresh}
        cellHeight={cellHeight}
        decimalPlaces={decimalPlaces}
        resizerTopHeight={resizerTopHeight}
        editQueryStatus={editQueryStatus}
        onSetResolution={onSetResolution}
      />
    )
  }

  if (type === 'table') {
    return (
      <RefreshingTableGraph
        type={type}
        source={source}
        cellID={cellID}
        colors={colors}
        inView={inView}
        isInCEO={isInCEO}
        key={manualRefresh}
        queries={queries}
        templates={templates}
        autoRefresh={autoRefresh}
        cellHeight={cellHeight}
        tableOptions={tableOptions}
        fieldOptions={fieldOptions}
        timeFormat={timeFormat}
        decimalPlaces={decimalPlaces}
        editQueryStatus={editQueryStatus}
        resizerTopHeight={resizerTopHeight}
        grabDataForDownload={grabDataForDownload}
        handleSetHoverTime={handleSetHoverTime}
        onSetResolution={onSetResolution}
      />
    )
  }

  const displayOptions = {
    stepPlot: type === 'line-stepplot',
    stackedGraph: type === 'line-stacked',
  }

  return (
    <RefreshingLineGraph
      type={type}
      axes={axes}
      source={source}
      cellID={cellID}
      colors={colors}
      onZoom={onZoom}
      queries={queries}
      inView={inView}
      key={manualRefresh}
      templates={templates}
      timeRange={timeRange}
      cellHeight={cellHeight}
      autoRefresh={autoRefresh}
      isBarGraph={type === 'bar'}
      decimalPlaces={decimalPlaces}
      staticLegend={staticLegend}
      displayOptions={displayOptions}
      editQueryStatus={editQueryStatus}
      grabDataForDownload={grabDataForDownload}
      handleSetHoverTime={handleSetHoverTime}
      showSingleStat={type === 'line-plus-single-stat'}
      onSetResolution={onSetResolution}
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
  type: string.isRequired,
  cellHeight: number,
  resizerTopHeight: number,
  axes: shape(),
  queries: arrayOf(shape()).isRequired,
  editQueryStatus: func,
  staticLegend: bool,
  onZoom: func,
  grabDataForDownload: func,
  colors: colorsStringSchema,
  cellID: string,
  inView: bool,
  tableOptions: shape({
    verticalTimeAxis: bool.isRequired,
    sortBy: shape({
      internalName: string.isRequired,
      displayName: string.isRequired,
      visible: bool.isRequired,
    }).isRequired,
    wrapping: string.isRequired,
    fixFirstColumn: bool.isRequired,
  }),
  fieldOptions: arrayOf(
    shape({
      internalName: string.isRequired,
      displayName: string.isRequired,
      visible: bool.isRequired,
    }).isRequired
  ),
  timeFormat: string.isRequired,
  decimalPlaces: shape({
    isEnforced: bool.isRequired,
    digits: number.isRequired,
  }).isRequired,
  handleSetHoverTime: func.isRequired,
  isInCEO: bool,
  onSetResolution: func,
  source: shape().isRequired,
}

RefreshingGraph.defaultProps = {
  manualRefresh: 0,
  staticLegend: false,
  inView: true,
  timeFormat: DEFAULT_TIME_FORMAT,
  decimalPlaces: DEFAULT_DECIMAL_PLACES,
}

const mapStateToProps = ({annotations: {mode}}) => ({
  mode,
})

const mapDispatchToProps = dispatch => ({
  handleSetHoverTime: bindActionCreators(setHoverTime, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(RefreshingGraph)
