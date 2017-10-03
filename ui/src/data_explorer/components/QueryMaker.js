import React, {PropTypes} from 'react'

import QueryEditor from './QueryEditor'
import SchemaExplorer from 'src/shared/components/SchemaExplorer'
import buildInfluxQLQuery from 'utils/influxql'

const rawTextBinder = (links, id, action) => text =>
  action(links.queries, id, text)

const buildText = (q, timeRange) =>
  q.rawText || buildInfluxQLQuery(timeRange, q) || ''

const QueryMaker = ({source, actions, timeRange, activeQuery}) =>
  <div className="query-maker query-maker--panel">
    <div className="query-maker--tab-contents">
      <QueryEditor
        query={buildText(activeQuery, timeRange)}
        config={activeQuery}
        onUpdate={rawTextBinder(
          source.links,
          activeQuery.id,
          actions.editRawTextAsync
        )}
      />
      <SchemaExplorer query={activeQuery} actions={actions} />
    </div>
  </div>

const {func, shape, string} = PropTypes

QueryMaker.propTypes = {
  source: shape({
    links: shape({
      queries: string.isRequired,
    }).isRequired,
  }).isRequired,
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
  activeQuery: shape({}),
}

export default QueryMaker
