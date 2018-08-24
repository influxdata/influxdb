import React, {SFC} from 'react'

import NewsFeed from 'src/status/components/NewsFeed'
import GettingStarted from 'src/status/components/GettingStarted'

import {Cell} from 'src/types/dashboards'
import {Source} from 'src/types/sources'
import {TimeRange} from 'src/types/queries'

interface Props {
  timeRange: TimeRange
  cell: Cell
  source: Source
}

const WidgetCell: SFC<Props> = ({cell, source}) => {
  switch (cell.type) {
    case 'news': {
      return <NewsFeed source={source} />
    }
    case 'guide': {
      return <GettingStarted />
    }
    default: {
      return (
        <div className="graph-empty">
          <p data-test="data-explorer-no-results">Nothing to show</p>
        </div>
      )
    }
  }
}

export default WidgetCell
