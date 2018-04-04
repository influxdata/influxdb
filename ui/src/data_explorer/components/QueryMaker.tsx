import React, {PureComponent} from 'react'

import QueryEditor from './QueryEditor'
import SchemaExplorer from 'src/shared/components/SchemaExplorer'
import {buildRawText} from 'src/utils/influxql'
import {Source, TimeRange, Query} from 'src/types'

const rawTextBinder = (links, id, action) => text =>
  action(links.queries, id, text)

interface Props {
  source: Source
  timeRange: TimeRange
  actions: any
  activeQuery: Query
  initialGroupByTime: string
}

class QueryMaker extends PureComponent<Props> {
  public render() {
    const {source, actions, activeQuery, initialGroupByTime} = this.props

    return (
      <div className="query-maker query-maker--panel">
        <div className="query-maker--tab-contents">
          <QueryEditor
            query={this.rawText}
            config={activeQuery}
            onUpdate={rawTextBinder(
              source.links,
              activeQuery.id,
              actions.editRawTextAsync
            )}
          />
          <SchemaExplorer
            source={source}
            query={activeQuery}
            actions={actions}
            initialGroupByTime={initialGroupByTime}
          />
        </div>
      </div>
    )
  }

  get rawText(): string {
    const {activeQuery, timeRange} = this.props
    return buildRawText(activeQuery, timeRange)
  }
}

export default QueryMaker
