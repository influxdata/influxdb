import React, {PropTypes} from 'react'
import QueryMakerTab from 'src/data_explorer/components/QueryMakerTab'
import buildInfluxQLQuery from 'utils/influxql'

const QueryTabList = ({
  queries,
  timeRange,
  onAddQuery,
  onDeleteQuery,
  activeQueryIndex,
  setActiveQueryIndex,
}) =>
  <div className="query-maker--tabs">
    {queries.map((q, i) =>
      <QueryMakerTab
        isActive={i === activeQueryIndex}
        key={i}
        queryIndex={i}
        query={q}
        onSelect={setActiveQueryIndex}
        onDelete={onDeleteQuery}
        queryTabText={
          q.rawText || buildInfluxQLQuery(timeRange, q) || `Query ${i + 1}`
        }
      />
    )}
    <div
      className="query-maker--new btn btn-sm btn-primary"
      onClick={onAddQuery}
    >
      <span className="icon plus" />
    </div>
  </div>

const {arrayOf, func, number, shape, string} = PropTypes

QueryTabList.propTypes = {
  queries: arrayOf(shape({})).isRequired,
  timeRange: shape({
    upper: string,
    lower: string,
  }).isRequired,
  onAddQuery: func.isRequired,
  onDeleteQuery: func.isRequired,
  activeQueryIndex: number.isRequired,
  setActiveQueryIndex: func.isRequired,
}

export default QueryTabList
