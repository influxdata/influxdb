import React, {PropTypes} from 'react'

import AutoRefresh from 'shared/components/AutoRefresh'
import LineGraph from 'shared/components/LineGraph'
import SingleStat from 'shared/components/SingleStat'

const RefreshingLineGraph = AutoRefresh(LineGraph)
const RefreshingSingleStat = AutoRefresh(SingleStat)

const RefreshingGraph = ({
  axes,
  type,
  onZoom,
  queries,
  templates,
  timeRange,
  cellHeight,
  autoRefresh,
  synchronizer,
  editQueryStatus,
}) => {
  if (type === 'single-stat') {
    return (
      <RefreshingSingleStat
        queries={[queries[0]]}
        templates={templates}
        autoRefresh={autoRefresh}
        cellHeight={cellHeight}
      />
    )
  }

  const displayOptions = {
    stepPlot: type === 'line-stepplot',
    stackedGraph: type === 'line-stacked',
  }

  return (
    <RefreshingLineGraph
      queries={queries}
      templates={templates}
      timeRange={timeRange}
      autoRefresh={autoRefresh}
      showSingleStat={type === 'line-plus-single-stat'}
      isBarGraph={type === 'bar'}
      displayOptions={displayOptions}
      synchronizer={synchronizer}
      editQueryStatus={editQueryStatus}
      axes={axes}
      onZoom={onZoom}
    />
  )
}

const {arrayOf, func, number, shape, string} = PropTypes

RefreshingGraph.propTypes = {
  timeRange: shape({
    lower: string.isRequired,
  }),
  autoRefresh: number.isRequired,
  templates: arrayOf(shape()),
  synchronizer: func,
  type: string.isRequired,
  cellHeight: number,
  axes: shape(),
  queries: arrayOf(shape()).isRequired,
  editQueryStatus: func,
  onZoom: func,
}

export default RefreshingGraph
