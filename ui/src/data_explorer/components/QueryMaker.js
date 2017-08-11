import React, {PropTypes} from 'react'

import QueryBuilder from './QueryBuilder'
import EmptyQuery from 'src/shared/components/EmptyQuery'
import QueryTabList from 'src/shared/components/QueryTabList'

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
    {activeQuery
      ? <QueryBuilder
          layout={'default'}
          source={source}
          actions={actions}
          query={activeQuery}
          timeRange={timeRange}
          onAddQuery={onAddQuery}
          isInDataExplorer={true}
        />
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
