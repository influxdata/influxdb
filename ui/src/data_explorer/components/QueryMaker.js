import React, {PropTypes} from 'react'

import QueryEditor from './QueryEditor'
import EmptyQuery from 'src/shared/components/EmptyQuery'
import QueryTabList from 'src/shared/components/QueryTabList'
import SchemaExplorer from 'src/shared/components/SchemaExplorer'
import buildInfluxQLQuery from 'utils/influxql'

const rawTextBinder = (links, id, action) => text =>
  action(links.queries, id, text)

const buildText = (q, timeRange) =>
  q.rawText || buildInfluxQLQuery(timeRange, q) || ''

const QueryMaker = ({
  source,
  actions,
  queries,
  timeRange,
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
          <QueryEditor
            query={buildText(activeQuery, timeRange)}
            config={activeQuery}
            onUpdate={rawTextBinder(
              source.links,
              activeQuery.id,
              actions.editRawTextAsync
            )}
          />
          <SchemaExplorer
            query={activeQuery}
            actions={actions}
            onAddQuery={onAddQuery}
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
    addQuery: func.isRequired,
    toggleField: func.isRequired,
    groupByTime: func.isRequired,
    toggleTagAcceptance: func.isRequired,
    applyFuncsToField: func.isRequired,
    editRawTextAsync: func.isRequired,
  }).isRequired,
  setActiveQueryIndex: func.isRequired,
  onDeleteQuery: func.isRequired,
  onAddQuery: func.isRequired,
  activeQuery: shape({}),
  activeQueryIndex: number,
}

export default QueryMaker
