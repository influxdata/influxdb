import React, {PropTypes} from 'react'

import Table from './Table'
import AutoRefresh from 'shared/components/AutoRefresh'
import LineGraph from 'shared/components/LineGraph'
import SingleStat from 'shared/components/SingleStat'
const RefreshingLineGraph = AutoRefresh(LineGraph)
const RefreshingSingleStat = AutoRefresh(SingleStat)

const VisView = ({
  view,
  queries,
  yRanges,
  cellType,
  templates,
  autoRefresh,
  heightPixels,
  editQueryStatus,
  activeQueryIndex,
  isInDataExplorer,
}) => {
  const activeQuery = queries[activeQueryIndex]
  const defaultQuery = queries[0]
  const query = activeQuery || defaultQuery

  if (view === 'table') {
    if (!query) {
      return (
        <div className="graph-empty">
          <p>Build a Query above</p>
        </div>
      )
    }

    return (
      <Table
        query={query}
        height={heightPixels}
        editQueryStatus={editQueryStatus}
      />
    )
  }

  if (cellType === 'single-stat') {
    return (
      <RefreshingSingleStat
        queries={queries.length ? [queries[0]] : []}
        autoRefresh={autoRefresh}
        templates={templates}
      />
    )
  }

  const displayOptions = {
    stepPlot: cellType === 'line-stepplot',
    stackedGraph: cellType === 'line-stacked',
  }

  return (
    <RefreshingLineGraph
      queries={queries}
      yRanges={yRanges}
      templates={templates}
      autoRefresh={autoRefresh}
      activeQueryIndex={activeQueryIndex}
      isInDataExplorer={isInDataExplorer}
      showSingleStat={cellType === 'line-plus-single-stat'}
      isBarGraph={cellType === 'bar'}
      displayOptions={displayOptions}
      editQueryStatus={editQueryStatus}
    />
  )
}

const {arrayOf, bool, func, number, shape, string} = PropTypes

VisView.propTypes = {
  view: string.isRequired,
  yRanges: shape(),
  queries: arrayOf(shape()).isRequired,
  cellType: string,
  templates: arrayOf(shape()),
  autoRefresh: number.isRequired,
  heightPixels: number,
  editQueryStatus: func.isRequired,
  activeQueryIndex: number,
  isInDataExplorer: bool,
}

export default VisView
