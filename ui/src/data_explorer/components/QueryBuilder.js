import React, {PropTypes} from 'react'

import QueryEditor from './QueryEditor'
import QueryTabItem from './QueryTabItem'
import buildInfluxQLQuery from 'utils/influxql'

const {arrayOf, func, node, number, shape, string} = PropTypes

const QueryBuilder = React.createClass({
  propTypes: {
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
    height: string,
    top: string,
    setActiveQueryIndex: func.isRequired,
    onDeleteQuery: func.isRequired,
    activeQueryIndex: number,
    children: node,
  },

  handleAddQuery() {
    const newIndex = this.props.queries.length
    this.props.actions.addQuery()
    this.props.setActiveQueryIndex(newIndex)
  },

  handleAddRawQuery() {
    const newIndex = this.props.queries.length
    this.props.actions.addQuery({rawText: ''})
    this.props.setActiveQueryIndex(newIndex)
  },

  getActiveQuery() {
    const {queries, activeQueryIndex} = this.props
    const activeQuery = queries[activeQueryIndex]
    const defaultQuery = queries[0]

    return activeQuery || defaultQuery
  },

  render() {
    const {height, top} = this.props
    return (
      <div className="query-builder" style={{height, top}}>
        {this.renderQueryTabList()}
        {this.renderQueryEditor()}
      </div>
    )
  },

  renderQueryEditor() {
    const {timeRange, actions, source, templates} = this.props
    const query = this.getActiveQuery()

    if (!query) {
      return (
        <div className="qeditor--empty">
          <h5 className="no-user-select">This Graph has no Queries</h5>
          <br />
          <div
            className="btn btn-primary"
            role="button"
            onClick={this.handleAddQuery}
          >
            Add a Query
          </div>
        </div>
      )
    }

    return (
      <QueryEditor
        source={source}
        timeRange={timeRange}
        templates={templates}
        query={query}
        actions={actions}
        onAddQuery={this.handleAddQuery}
      />
    )
  },

  renderQueryTabList() {
    const {
      queries,
      activeQueryIndex,
      onDeleteQuery,
      timeRange,
      setActiveQueryIndex,
    } = this.props

    return (
      <div className="query-builder--tabs">
        <div className="query-builder--tabs-heading">
          <h1>Queries</h1>
          <div
            className="panel--tab-new btn btn-sm btn-primary dropdown-toggle"
            onClick={this.handleAddQuery}
          >
            <span className="icon plus" />
          </div>
        </div>
        {queries.map((q, i) => {
          return (
            <QueryTabItem
              isActive={i === activeQueryIndex}
              key={i}
              queryIndex={i}
              query={q}
              onSelect={setActiveQueryIndex}
              onDelete={onDeleteQuery}
              queryTabText={
                q.rawText ||
                  buildInfluxQLQuery(timeRange, q) ||
                  `Query ${i + 1}`
              }
            />
          )
        })}
        {this.props.children}
      </div>
    )
  },
})

export default QueryBuilder
