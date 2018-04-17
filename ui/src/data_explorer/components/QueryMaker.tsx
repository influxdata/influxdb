import React, {PureComponent} from 'react'

import QueryEditor from './QueryEditor'
import SchemaExplorer from 'src/shared/components/SchemaExplorer'
import {Source, Query} from 'src/types'
import {ErrorHandling} from 'src/shared/decorators/errors'

const rawTextBinder = (links, id, action) => text =>
  action(links.queries, id, text)

interface Props {
  source: Source
  rawText: string
  actions: any
  activeQuery: Query
  initialGroupByTime: string
}

@ErrorHandling
class QueryMaker extends PureComponent<Props> {
  public render() {
    const {
      source,
      actions,
      rawText,
      activeQuery,
      initialGroupByTime,
    } = this.props

    return (
      <div className="query-maker query-maker--panel">
        <div className="query-maker--tab-contents">
          <QueryEditor
            query={rawText}
            config={activeQuery}
            onUpdate={rawTextBinder(
              source.links,
              activeQuery.id,
              actions.editRawTextAsync
            )}
          />
          <SchemaExplorer
            source={source}
            actions={actions}
            query={activeQuery}
            initialGroupByTime={initialGroupByTime}
          />
        </div>
      </div>
    )
  }
}

export default QueryMaker
