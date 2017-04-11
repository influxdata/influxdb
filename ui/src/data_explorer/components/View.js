import React, {PropTypes} from 'react'

import Table from './Table'
import AutoRefresh from 'shared/components/AutoRefresh'
import LineGraph from 'shared/components/LineGraph'
import SingleStat from 'shared/components/SingleStat'
const RefreshingLineGraph = AutoRefresh(LineGraph)
const RefreshingSingleStat = AutoRefresh(SingleStat)

const View = ({
  view,
  queries,
  cellType,
  autoRefresh,
  heightPixels,
  editQueryStatus,
  fetchTimeSeries,
  activeQueryIndex,
}) => {
  const activeQuery = queries[activeQueryIndex]
  const defaultQuery = queries[0]
  const query = activeQuery || defaultQuery

  if (view === 'table') {
    if (!query) {
      return <div className="generic-empty-state">Enter your query above</div>
    }

    return (
      <Table
        query={query}
        height={heightPixels}
        editQueryStatus={editQueryStatus}
        fetchTimeSeries={fetchTimeSeries}
      />
    )
  }

  if (cellType === 'single-stat') {
    return <RefreshingSingleStat queries={[queries[0]]} autoRefresh={autoRefresh} />
  }

  const displayOptions = {
    stepPlot: cellType === 'line-stepplot',
    stackedGraph: cellType === 'line-stacked',
  }

  return (
    <RefreshingLineGraph
      queries={queries}
      autoRefresh={autoRefresh}
      activeQueryIndex={activeQueryIndex}
      isInDataExplorer={true}
      showSingleStat={cellType === "line-plus-single-stat"}
      displayOptions={displayOptions}
      fetchTimeSeries={fetchTimeSeries}
      editQueryStatus={editQueryStatus}
    />
  )
}

const {
  arrayOf,
  func,
  number,
  shape,
  string,
} = PropTypes

View.propTypes = {
  view: string.isRequired,
  queries: arrayOf(shape()).isRequired,
  cellType: string,
  autoRefresh: number.isRequired,
  heightPixels: number,
  editQueryStatus: func,
  fetchTimeSeries: func.isRequired,
  activeQueryIndex: number,
}

export default View
