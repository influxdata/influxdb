import React, {PropTypes} from 'react'

import QueryEditor from './QueryEditor'
import QueryTabItem from './QueryTabItem'
import SimpleDropdown from 'src/shared/components/SimpleDropdown'
import buildInfluxQLQuery from 'utils/influxql'

const {
  arrayOf,
  func,
  node,
  number,
  shape,
  string,
} = PropTypes

const QueryBuilder = React.createClass({
  propTypes: {
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
    }).isRequired,
    height: string,
    top: string,
    setActiveQueryIndex: func.isRequired,
    onDeleteQuery: func.isRequired,
    activeQueryIndex: number,
    children: node,
  },

  handleSetActiveQueryIndex(index) {
    this.props.setActiveQueryIndex(index)
  },

  handleAddQuery() {
    const newIndex = this.props.queries.length
    this.props.actions.addQuery()
    this.handleSetActiveQueryIndex(newIndex)
  },

  handleAddRawQuery() {
    const newIndex = this.props.queries.length
    this.props.actions.addQuery({rawText: `SELECT "fields" from "db"."rp"."measurement"`})
    this.handleSetActiveQueryIndex(newIndex)
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
    const {timeRange, actions} = this.props
    const query = this.getActiveQuery()

    if (!query) {
      return (
        <div className="qeditor--empty">
          <h5 className="no-user-select">This Graph has no Queries</h5>
          <br/>
          <div className="btn btn-primary" role="button" onClick={this.handleAddQuery}>Add a Query</div>
        </div>
      )
    }

    return (
      <QueryEditor
        timeRange={timeRange}
        query={query}
        actions={actions}
        onAddQuery={this.handleAddQuery}
      />
    )
  },

  renderQueryTabList() {
    const {queries, activeQueryIndex, onDeleteQuery, timeRange} = this.props
    return (
      <div className="query-builder--tabs">
        <div className="query-builder--tabs-heading">
          <h1>Queries</h1>
          {this.renderAddQuery()}
        </div>
        {queries.map((q, i) => {
          return (
            <QueryTabItem
              isActive={i === activeQueryIndex}
              key={i}
              queryIndex={i}
              query={q}
              onSelect={this.handleSetActiveQueryIndex}
              onDelete={onDeleteQuery}
              queryTabText={q.rawText || buildInfluxQLQuery(timeRange, q) || `SELECT "fields" FROM "db"."rp"."measurement"`}
            />
          )
        })}
        {this.props.children}
      </div>
    )
  },

  onChoose(item) {
    switch (item.text) {
      case 'Query Builder':
        this.handleAddQuery()
        break
      case 'Query Editor':
        this.handleAddRawQuery()
        break
    }
  },

  renderAddQuery() {
    const items = [{text: 'Query Builder'}, {text: 'Query Editor'}]
    return (
      <SimpleDropdown onChoose={this.onChoose} items={items} className="panel--tab-new">
        <span className="icon plus"></span>
      </SimpleDropdown>
    )
  },
})

export default QueryBuilder
