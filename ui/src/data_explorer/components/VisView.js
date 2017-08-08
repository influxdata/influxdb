import React, {PropTypes} from 'react'

import Table from './Table'
import RefreshingGraph from 'shared/components/RefreshingGraph'

const VisView = ({
  axes,
  view,
  queries,
  cellType,
  templates,
  autoRefresh,
  heightPixels,
  editQueryStatus,
  activeQueryIndex,
  resizerBottomHeight,
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
        height={resizerBottomHeight}
        editQueryStatus={editQueryStatus}
      />
    )
  }

  return (
    <RefreshingGraph
      axes={axes}
      type={cellType}
      queries={queries}
      templates={templates}
      cellHeight={heightPixels}
      autoRefresh={autoRefresh}
    />
  )
}

const {arrayOf, func, number, shape, string} = PropTypes

VisView.propTypes = {
  view: string.isRequired,
  axes: shape(),
  queries: arrayOf(shape()).isRequired,
  cellType: string,
  templates: arrayOf(shape()),
  autoRefresh: number.isRequired,
  heightPixels: number,
  editQueryStatus: func.isRequired,
  activeQueryIndex: number,
  resizerBottomHeight: number,
}

export default VisView
