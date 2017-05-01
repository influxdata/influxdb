import React, {PropTypes} from 'react'

import DatabaseList from './DatabaseList'
import MeasurementList from './MeasurementList'
import FieldList from './FieldList'
import QueryEditor from './QueryEditor'
import buildInfluxQLQuery from 'utils/influxql'

const {arrayOf, func, shape, string} = PropTypes

const QueryBuilder = React.createClass({
  propTypes: {
    source: shape({
      links: shape({
        queries: string.isRequired,
      }).isRequired,
    }).isRequired,
    query: shape({
      id: string,
    }).isRequired,
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
      applyFuncsToField: func.isRequired,
      chooseTag: func.isRequired,
      groupByTag: func.isRequired,
      toggleField: func.isRequired,
      groupByTime: func.isRequired,
      toggleTagAcceptance: func.isRequired,
      editRawTextAsync: func.isRequired,
    }).isRequired,
  },

  handleChooseNamespace(namespace) {
    this.props.actions.chooseNamespace(this.props.query.id, namespace)
  },

  handleChooseMeasurement(measurement) {
    this.props.actions.chooseMeasurement(this.props.query.id, measurement)
  },

  handleToggleField(field) {
    this.props.actions.toggleField(this.props.query.id, field)
  },

  handleGroupByTime(time) {
    this.props.actions.groupByTime(this.props.query.id, time)
  },

  handleApplyFuncsToField(fieldFunc) {
    this.props.actions.applyFuncsToField(this.props.query.id, fieldFunc)
  },

  handleChooseTag(tag) {
    this.props.actions.chooseTag(this.props.query.id, tag)
  },

  handleToggleTagAcceptance() {
    this.props.actions.toggleTagAcceptance(this.props.query.id)
  },

  handleGroupByTag(tagKey) {
    this.props.actions.groupByTag(this.props.query.id, tagKey)
  },

  handleEditRawText(text) {
    const {source: {links}, query} = this.props
    this.props.actions.editRawTextAsync(links.queries, query.id, text)
  },

  render() {
    const {query, timeRange, templates} = this.props
    const q = query.rawText || buildInfluxQLQuery(timeRange, query) || ''

    return (
      <div className="query-maker--tab-contents">
        <QueryEditor
          query={q}
          config={query}
          onUpdate={this.handleEditRawText}
          templates={templates}
        />
        {this.renderLists()}
      </div>
    )
  },

  renderLists() {
    const {query} = this.props

    return (
      <div className="query-builder">
        <DatabaseList
          query={query}
          onChooseNamespace={this.handleChooseNamespace}
        />
        <MeasurementList
          query={query}
          onChooseMeasurement={this.handleChooseMeasurement}
          onChooseTag={this.handleChooseTag}
          onToggleTagAcceptance={this.handleToggleTagAcceptance}
          onGroupByTag={this.handleGroupByTag}
        />
        <FieldList
          query={query}
          onToggleField={this.handleToggleField}
          onGroupByTime={this.handleGroupByTime}
          applyFuncsToField={this.handleApplyFuncsToField}
        />
      </div>
    )
  },
})

export default QueryBuilder
