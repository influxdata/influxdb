import React, {PropTypes} from 'react'

import EmptyQuery from 'src/shared/components/EmptyQuery'
import QueryTabList from 'src/shared/components/QueryTabList'
import QueryTextArea from 'src/dashboards/components/QueryTextArea'
import SchemaExplorer from 'src/dashboards/components/SchemaExplorer'
import buildInfluxQLQuery from 'utils/influxql'

const TEMPLATE_RANGE = {upper: null, lower: ':dashboardTime:'}
const rawTextBinder = (links, id, action) => text =>
  action(links.queries, id, text)
const buildText = q =>
  q.rawText || buildInfluxQLQuery(q.range || TEMPLATE_RANGE, q) || ''

const QueryMaker = ({
  source: {links},
  actions,
  queries,
  timeRange,
  templates,
  onAddQuery,
  activeQuery,
  onDeleteQuery,
  activeQueryIndex,
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
              links,
              activeQuery.id,
              actions.editRawTextAsync
            )}
            templates={templates}
          />
          <SchemaExplorer
            query={activeQuery}
            actions={actions}
            onAddQuery={onAddQuery}
          />
        </div>
      : <EmptyQuery onAddQuery={onAddQuery} />}
  </div>

const {arrayOf, bool, func, number, shape, string} = PropTypes

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
  isInDataExplorer: bool,
  actions: shape({
    chooseNamespace: func.isRequired,
    chooseMeasurement: func.isRequired,
    chooseTag: func.isRequired,
    groupByTag: func.isRequired,
    toggleField: func.isRequired,
    groupByTime: func.isRequired,
    toggleTagAcceptance: func.isRequired,
    applyFuncsToField: func.isRequired,
    editRawTextAsync: func.isRequired,
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
}

export default QueryMaker
