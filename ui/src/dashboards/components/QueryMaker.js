import React, {PropTypes} from 'react'

import QueryBuilder from 'src/data_explorer/components/QueryBuilder'
import QueryTabList from 'src/dashboards/components/QueryTabList'
import classnames from 'classnames'

const {arrayOf, bool, func, node, number, shape, string} = PropTypes

const QueryMaker = React.createClass({
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
    height: string,
    top: string,
    setActiveQueryIndex: func.isRequired,
    onDeleteQuery: func.isRequired,
    activeQueryIndex: number,
    children: node,
    layout: string,
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
    const {
      height,
      top,
      layout,
      queries,
      timeRange,
      onDeleteQuery,
      activeQueryIndex,
      setActiveQueryIndex,
    } = this.props

    return (
      <div
        className={classnames('query-maker', {
          'query-maker--panel': layout === 'panel',
        })}
        style={{height, top}}
      >
        <QueryTabList
          queries={queries}
          timeRange={timeRange}
          onAddQuery={this.handleAddQuery}
          onDeleteQuery={onDeleteQuery}
          activeQueryIndex={activeQueryIndex}
          setActiveQueryIndex={setActiveQueryIndex}
        />
        {this.renderQueryBuilder()}
      </div>
    )
  },

  renderQueryBuilder() {
    const {
      timeRange,
      actions,
      source,
      templates,
      layout,
      isInDataExplorer,
    } = this.props
    const query = this.getActiveQuery()

    if (!query) {
      return (
        <div className="query-maker--empty">
          <h5>This Graph has no Queries</h5>
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
      <QueryBuilder
        source={source}
        timeRange={timeRange}
        templates={templates}
        query={query}
        actions={actions}
        onAddQuery={this.handleAddQuery}
        layout={layout}
        isInDataExplorer={isInDataExplorer}
      />
    )
  },
})

QueryMaker.defaultProps = {
  layout: 'default',
}
export default QueryMaker
