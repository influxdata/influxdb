import React, {PropTypes} from 'react'

import QueryBuilder from 'src/data_explorer/components/QueryBuilder'
import QueryTabList from 'src/dashboards/components/QueryTabList'
import EmptyQuery from 'src/dashboards/components/EmptyQuery'

const QueryMaker = ({
  layout,
  source,
  actions,
  queries,
  timeRange,
  templates,
  onDeleteQuery,
  activeQueryIndex,
  setActiveQueryIndex,
}) => {
  const handleAddQuery = () => {
    const newIndex = queries.length
    actions.addQuery()
    setActiveQueryIndex(newIndex)
  }

  const getActiveQuery = () => {
    const activeQuery = queries[activeQueryIndex]
    const defaultQuery = queries[0]

    return activeQuery || defaultQuery
  }

  const query = getActiveQuery()

  return (
    <div className="query-maker query-maker--panel">
      <QueryTabList
        queries={queries}
        timeRange={timeRange}
        onAddQuery={handleAddQuery}
        onDeleteQuery={onDeleteQuery}
        activeQueryIndex={activeQueryIndex}
        setActiveQueryIndex={setActiveQueryIndex}
      />
      {query
        ? <QueryBuilder
            query={query}
            layout={layout}
            source={source}
            actions={actions}
            timeRange={timeRange}
            templates={templates}
            onAddQuery={handleAddQuery}
          />
        : <EmptyQuery onAddQuery={handleAddQuery} />}
    </div>
  )
}

const {arrayOf, bool, func, node, number, shape, string} = PropTypes

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
    addQuery: func.isRequired,
    toggleField: func.isRequired,
    groupByTime: func.isRequired,
    toggleTagAcceptance: func.isRequired,
    applyFuncsToField: func.isRequired,
    editRawTextAsync: func.isRequired,
  }).isRequired,
  setActiveQueryIndex: func.isRequired,
  onDeleteQuery: func.isRequired,
  activeQueryIndex: number,
  children: node,
  layout: string,
}

QueryMaker.defaultProps = {
  layout: 'default',
}

export default QueryMaker
