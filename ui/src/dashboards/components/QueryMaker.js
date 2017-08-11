import React, {PropTypes} from 'react'

import EmptyQuery from 'src/dashboards/components/EmptyQuery'
import QueryTabList from 'src/dashboards/components/QueryTabList'
import QueryTextArea from 'src/dashboards/components/QueryTextArea'
import SchemaExplorer from 'src/dashboards/components/SchemaExplorer'
import buildInfluxQLQuery from 'utils/influxql'

const TEMPLATE_RANGE = {upper: null, lower: ':dashboardTime:'}
const rawTextBinder = (links, id, action) => text =>
  action(links.queries, id, text)
const buildText = (rawText, range, q) =>
  rawText || buildInfluxQLQuery(range || TEMPLATE_RANGE, q) || ''

const QueryMaker = ({
  source: {links},
  actions,
  queries,
  timeRange,
  templates,
  onAddQuery,
  activeQuery,
  activeQuery: {id, range, rawText},
  onDeleteQuery,
  activeQueryIndex,
  setActiveQueryIndex,
}) => {
  return (
    <div className="query-maker query-maker--panel">
      <QueryTabList
        queries={queries}
        timeRange={timeRange}
        onAddQuery={onAddQuery}
        onDeleteQuery={onDeleteQuery}
        activeQueryIndex={activeQueryIndex}
        setActiveQueryIndex={setActiveQueryIndex}
      />
      {activeQuery
        ? <div className="query-maker--tab-contents">
            <QueryTextArea
              query={buildText(rawText, range, activeQuery)}
              config={activeQuery}
              onUpdate={rawTextBinder(links, id, actions.editRawTextAsync)}
              templates={templates}
            />
            <SchemaExplorer
              query={activeQuery}
              actions={actions}
              templates={templates}
              onAddQuery={onAddQuery}
            />
          </div>
        : <EmptyQuery onAddQuery={onAddQuery} />}
    </div>
  )
}

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
  templates: arrayOf(
    shape({
      tempVar: string.isRequired,
    })
  ),
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
}

export default QueryMaker
