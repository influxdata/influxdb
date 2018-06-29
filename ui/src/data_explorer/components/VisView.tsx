import React, {SFC} from 'react'

import Table from './Table'
import RefreshingGraph from 'src/shared/components/RefreshingGraph'
import {DEFAULT_LINE_COLORS} from 'src/shared/constants/graphColorPalettes'

import {Source, Query, Template} from 'src/types'

interface Props {
  view: string
  query?: Query
  source: Source
  queries: Query[]
  templates: Template[]
  autoRefresh: number
  editQueryStatus: () => void
  manualRefresh: number
}

const DataExplorerVisView: SFC<Props> = ({
  view,
  query,
  source,
  queries,
  templates,
  autoRefresh,
  manualRefresh,
  editQueryStatus,
}) => {
  if (view === 'table') {
    if (!query) {
      return (
        <div className="graph-empty">
          <p> Build a Query above </p>
        </div>
      )
    }

    return (
      <Table query={query} editQueryStatus={editQueryStatus} source={source} />
    )
  }

  return (
    <RefreshingGraph
      type="line-graph"
      source={source}
      queries={queries}
      templates={templates}
      autoRefresh={autoRefresh}
      colors={DEFAULT_LINE_COLORS}
      manualRefresh={manualRefresh}
      editQueryStatus={editQueryStatus}
    />
  )
}

export default DataExplorerVisView
