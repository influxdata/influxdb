import React, {PropTypes} from 'react'

import FieldListItem from './FieldListItem'
import GroupByTimeDropdown from './GroupByTimeDropdown'

import {showFieldKeys} from 'shared/apis/metaQuery'
import showFieldKeysParser from 'shared/parsing/showFieldKeys'

const {string, shape, func} = PropTypes
const FieldList = React.createClass({
  propTypes: {
    query: shape({
      database: string,
      retentionPolicy: string,
      measurement: string,
    }).isRequired,
    onToggleField: func.isRequired,
    onGroupByTime: func.isRequired,
    applyFuncsToField: func.isRequired,
    isKapacitorRule: PropTypes.bool,
  },

  getDefaultProps() {
    return {
      isKapacitorRule: false,
    }
  },

  contextTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      fields: [],
    }
  },

  componentDidMount() {
    const {database, measurement} = this.props.query
    if (!database || !measurement) {
      return
    }

    this._getFields()
  },

  componentDidUpdate(prevProps) {
    const {database, measurement, retentionPolicy} = this.props.query
    const {database: prevDB, measurement: prevMeas, retentionPolicy: prevRP} = prevProps.query
    if (!database || !measurement) {
      return
    }

    if (database === prevDB && measurement === prevMeas && retentionPolicy === prevRP) {
      return
    }

    this._getFields()
  },

  handleGroupByTime(groupBy) {
    this.props.onGroupByTime(groupBy.menuOption)
  },

  render() {
    const {query} = this.props
    const hasAggregates = query.fields.some((f) => f.funcs && f.funcs.length)
    const hasGroupByTime = query.groupBy.time

    return (
      <div className="query-builder--column">
        <div className="query-builder--heading">Fields</div>
        {
          hasAggregates ?
            <div className="qeditor--list-header">
              <div className="group-by-time">
                <p>Group by Time</p>
                <GroupByTimeDropdown isOpen={!hasGroupByTime} selected={query.groupBy.time} onChooseGroupByTime={this.handleGroupByTime} />
              </div>
            </div>
            : null
        }
        {this.renderList()}
      </div>
    )
  },

  renderList() {
    const {database, measurement} = this.props.query
    if (!database || !measurement) {
      return (
        <div className="query-builder--list-empty">
          <span>No <strong>Measurement</strong> selected</span>
        </div>
      )
    }

    return (
      <div className="query-builder--list">
        {this.state.fields.map((fieldFunc) => {
          const selectedField = this.props.query.fields.find((f) => f.field === fieldFunc.field)
          return (
            <FieldListItem
              key={fieldFunc.field}
              onToggleField={this.props.onToggleField}
              onApplyFuncsToField={this.props.applyFuncsToField}
              isSelected={!!selectedField}
              fieldFunc={selectedField || fieldFunc}
              isKapacitorRule={this.props.isKapacitorRule}
            />
          )
        })}
      </div>
    )
  },

  _getFields() {
    const {database, measurement, retentionPolicy} = this.props.query
    const {source} = this.context
    const proxySource = source.links.proxy

    showFieldKeys(proxySource, database, measurement, retentionPolicy).then((resp) => {
      const {errors, fieldSets} = showFieldKeysParser(resp.data)
      if (errors.length) {
        // TODO: do something
      }

      this.setState({
        fields: fieldSets[measurement].map((f) => {
          return {field: f, funcs: []}
        }),
      })
    })
  },
})

export default FieldList
