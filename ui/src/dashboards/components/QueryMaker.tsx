import React, {SFC} from 'react'
import _ from 'lodash'

import EmptyQuery from 'src/shared/components/EmptyQuery'
import QueryTabList from 'src/shared/components/QueryTabList'
import QueryTextArea from 'src/dashboards/components/QueryTextArea'
import SchemaExplorer from 'src/shared/components/SchemaExplorer'
import {buildQuery} from 'src/utils/influxql'
import {TYPE_QUERY_CONFIG} from 'src/dashboards/constants'
import {TEMPLATE_RANGE} from 'src/tempVars/constants'

import {QueryConfig, Source, SourceLinks, TimeRange} from 'src/types'
import {CellEditorOverlayActions} from 'src/dashboards/components/CellEditorOverlay'

const rawTextBinder = (
  links: SourceLinks,
  id: string,
  action: (linksQueries: string, id: string, text: string) => void
) => (text: string) => action(links.queries, id, text)

const buildText = (q: QueryConfig): string =>
  q.rawText || buildQuery(TYPE_QUERY_CONFIG, q.range || TEMPLATE_RANGE, q) || ''

interface Template {
  tempVar: string
}

interface Props {
  source: Source
  queries: QueryConfig[]
  timeRange: TimeRange
  actions: CellEditorOverlayActions
  setActiveQueryIndex: (index: number) => void
  onDeleteQuery: (index: number) => void
  activeQueryIndex: number
  activeQuery: QueryConfig
  onAddQuery: () => void
  templates: Template[]
  initialGroupByTime: string
}

const QueryMaker: SFC<Props> = ({
  source,
  actions,
  queries,
  timeRange,
  templates,
  onAddQuery,
  activeQuery,
  onDeleteQuery,
  activeQueryIndex,
  initialGroupByTime,
  setActiveQueryIndex,
}) => (
  <div className="query-maker query-maker--panel">
    <QueryTabList
      queries={queries}
      timeRange={timeRange}
      onAddQuery={onAddQuery}
      onDeleteQuery={onDeleteQuery}
      activeQueryIndex={activeQueryIndex}
      setActiveQueryIndex={setActiveQueryIndex}
    />
    {activeQuery && activeQuery.id ? (
      <div className="query-maker--tab-contents">
        <QueryTextArea
          query={buildText(activeQuery)}
          config={activeQuery}
          onUpdate={rawTextBinder(
            source.links,
            activeQuery.id,
            actions.editRawTextAsync
          )}
          templates={templates}
        />
        <SchemaExplorer
          source={source}
          actions={actions}
          query={activeQuery}
          initialGroupByTime={initialGroupByTime}
          isQuerySupportedByExplorer={_.get(
            activeQuery,
            'isQuerySupportedByExplorer',
            true
          )}
        />
      </div>
    ) : (
      <EmptyQuery onAddQuery={onAddQuery} />
    )}
  </div>
)

export default QueryMaker
