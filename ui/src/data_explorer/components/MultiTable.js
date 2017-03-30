import React, {PropTypes} from 'react'
import Table from './Table'
import classNames from 'classnames'

const {
  arrayOf,
  bool,
  func,
  number,
  shape,
  string,
} = PropTypes

const MultiTable = React.createClass({
  propTypes: {
    queries: arrayOf(shape({
      host: arrayOf(string.isRequired).isRequired,
      text: string.isRequired,
    })),
    height: number,
    onEditRawStatus: func.isRequired,
    activeQueryIndex: number,
    onSetActiveQueryIndex: func.isRequired,
  },

  getActiveQuery() {
    const {queries, activeQueryIndex} = this.props
    const activeQuery = queries[activeQueryIndex]
    const defaultQuery = queries[0]

    return activeQuery || defaultQuery
  },

  render() {
    return (
      <div>
        {this.renderTabs()}
        {this.renderTable()}
      </div>
    )
  },

  renderTable() {
    const {height, onEditRawStatus} = this.props
    const query = this.getActiveQuery()
    const noQuery = !query || !query.text
    if (noQuery) {
      return null
    }

    return <Table key={query.text} query={query} height={height} onEditRawStatus={onEditRawStatus} />
  },

  renderTabs() {
    const {queries, onSetActiveQueryIndex} = this.props
    return (
      <div className="multi-table__tabs">
        {queries.map((q, i) => {
          return (
            <TabItem
              isActive={this.getActiveQuery().id === q.id}
              key={q.id}
              query={q}
              onSelect={() => onSetActiveQueryIndex(i)}
            />
          )
        })}
      </div>
    )
  },
})

const TabItem = React.createClass({
  propTypes: {
    query: shape({
      text: string.isRequired,
      id: string.isRequired,
      host: arrayOf(string.isRequired).isRequired,
    }).isRequired,
    onSelect: func.isRequired,
    isActive: bool.isRequired,
  },

  render() {
    const {isActive, onSelect} = this.props
    return (
      <div className={classNames("multi-table__tab", {active: isActive})} onClick={onSelect}>
        {"Query"}
      </div>
    )
  },
})

export default MultiTable
