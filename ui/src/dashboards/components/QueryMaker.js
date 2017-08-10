import React, {PropTypes} from 'react'

import EmptyQuery from 'src/dashboards/components/EmptyQuery'
import QueryTabList from 'src/dashboards/components/QueryTabList'
import SchemaExplorer from 'src/dashboards/components/SchemaExplorer'

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
        ? <SchemaExplorer
            query={activeQuery}
            source={source}
            actions={actions}
            templates={templates}
            onAddQuery={onAddQuery}
          />
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
