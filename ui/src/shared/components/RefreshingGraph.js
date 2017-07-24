import React, {PropTypes} from 'react'

import AutoRefresh from 'shared/components/AutoRefresh'
import LineGraph from 'shared/components/LineGraph'
import SingleStat from 'shared/components/SingleStat'

const RefreshingLineGraph = AutoRefresh(LineGraph)
const RefreshingSingleStat = AutoRefresh(SingleStat)

const RefreshingGraph = ({
  timeRange,
  autoRefresh,
  templates,
  synchronizer,
  type,
  queries,
  cellHeight,
  axes,
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
      axes={axes}
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
  queries: arrayOf(shape()).isRequired,
  cellHeight: number,
  axes: shape(),
}

export default RefreshingGraph
