import React, {PropTypes} from 'react'

import Table from './Table'
import RefreshingGraph from 'shared/components/RefreshingGraph'

const VisView = ({
  axes,
  view,
  query,
  queries,
  cellType,
  templates,
  autoRefresh,
  heightPixels,
  editQueryStatus,
  resizerBottomHeight,
}) => {
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
      editQueryStatus={editQueryStatus}
    />
  )
}

const {arrayOf, func, number, shape, string} = PropTypes

VisView.propTypes = {
  view: string.isRequired,
  axes: shape(),
  query: shape(),
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
