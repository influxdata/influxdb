import React, {PropTypes} from 'react'

import EmptyQuery from 'src/shared/components/EmptyQuery'
import QueryTabList from 'src/shared/components/QueryTabList'
import QueryTextArea from 'src/dashboards/components/QueryTextArea'
import SchemaExplorer from 'src/shared/components/SchemaExplorer'
import {buildQuery} from 'utils/influxql'
import {TYPE_QUERY_CONFIG} from 'src/dashboards/constants'

const TEMPLATE_RANGE = {upper: null, lower: ':dashboardTime:'}
const rawTextBinder = (links, id, action) => text =>
  action(links.queries, id, text)
const buildText = q =>
  q.rawText || buildQuery(TYPE_QUERY_CONFIG, q.range || TEMPLATE_RANGE, q) || ''

const QueryMaker = ({
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
}) =>
  <div className="query-maker query-maker--panel">
    <QueryTabList
      queries={queries}
      timeRange={timeRange}
      onAddQuery={onAddQuery}
      onDeleteQuery={onDeleteQuery}
      activeQueryIndex={activeQueryIndex}
      setActiveQueryIndex={setActiveQueryIndex}
    />
    {activeQuery && activeQuery.id
      ? <div className="query-maker--tab-contents">
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
            onAddQuery={onAddQuery}
            initialGroupByTime={initialGroupByTime}
          />
        </div>
      : <EmptyQuery onAddQuery={onAddQuery} />}
  </div>

const {arrayOf, func, number, shape, string} = PropTypes

QueryMaker.propTypes = {
  source: shape({
    links: shape({
      queries: string.isRequired,
    }).isRequired,
  }).isRequired,
  queries: arrayOf(shape({})).isRequired,
  timeRange: shape({
    upper: string,
    lower: string,
  }).isRequired,
  actions: shape({
    chooseNamespace: func.isRequired,
    chooseMeasurement: func.isRequired,
    chooseTag: func.isRequired,
    groupByTag: func.isRequired,
    toggleField: func.isRequired,
    groupByTime: func.isRequired,
    toggleTagAcceptance: func.isRequired,
    fill: func,
    applyFuncsToField: func.isRequired,
    editRawTextAsync: func.isRequired,
    addInitialField: func.isRequired,
  }).isRequired,
  setActiveQueryIndex: func.isRequired,
  onDeleteQuery: func.isRequired,
  activeQueryIndex: number,
  activeQuery: shape({}),
  onAddQuery: func.isRequired,
  templates: arrayOf(
    shape({
      tempVar: string.isRequired,
    })
  ).isRequired,
  initialGroupByTime: string.isRequired,
}

export default QueryMaker
