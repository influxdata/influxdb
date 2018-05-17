import React, {SFC} from 'react'

import AlertsApp from 'src/alerts/containers/AlertsApp'
import NewsFeed from 'src/status/components/NewsFeed'
import GettingStarted from 'src/status/components/GettingStarted'

import {Cell} from 'src/types/dashboard'
import {Source} from 'src/types/sources'
import {TimeRange} from 'src/types/query'
import {RECENT_ALERTS_LIMIT} from 'src/status/constants'

interface Props {
  timeRange: TimeRange
  cell: Cell
  source: Source
}

const WidgetCell: SFC<Props> = ({cell, source, timeRange}) => {
  switch (cell.type) {
    case 'alerts': {
      return (
        <AlertsApp
          source={source}
          timeRange={timeRange}
          isWidget={true}
          limit={RECENT_ALERTS_LIMIT}
        />
      )
    }
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
