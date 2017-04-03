import React, {PropTypes} from 'react'
import buildInfluxQLQuery from 'utils/influxql'

import DatabaseList from './DatabaseList'
import MeasurementList from './MeasurementList'
import FieldList from './FieldList'
import TagList from './TagList'
import RawQueryEditor from './RawQueryEditor'

const {
  string,
  shape,
  func,
} = PropTypes

const QueryEditor = React.createClass({
  propTypes: {
    query: shape({
      id: string,
    }).isRequired,
    timeRange: shape({
      upper: string,
      lower: string,
    }).isRequired,
    actions: shape({
      chooseNamespace: func.isRequired,
      chooseMeasurement: func.isRequired,
      applyFuncsToField: func.isRequired,
      chooseTag: func.isRequired,
      groupByTag: func.isRequired,
      toggleField: func.isRequired,
      groupByTime: func.isRequired,
      toggleTagAcceptance: func.isRequired,
      editRawText: func.isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      database: null,
      measurement: null,
    }
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
    this.props.actions.editRawText(this.props.query.id, text)
  },

  render() {
    return (
      <div className="query-builder--tab-contents">
        <div>
          {this.renderQuery()}
          {this.renderLists()}
        </div>
      </div>
    )
  },

  renderQuery() {
    const {query, timeRange} = this.props
    const statement = query.rawText || buildInfluxQLQuery(timeRange, query) || `Select a database, measurement, and field below.`

    if (typeof query.rawText !== 'string') {
      return (
        <div className="query-builder--query-preview">
          <pre><code>{statement}</code></pre>
        </div>
      )
    }

    return <RawQueryEditor query={query} onUpdate={this.handleEditRawText} />
  },

  renderLists() {
    const {query} = this.props

    return (
      <div className="query-builder--columns">
        <DatabaseList
          query={query}
          onChooseNamespace={this.handleChooseNamespace}
        />
        <MeasurementList
          query={query}
          onChooseMeasurement={this.handleChooseMeasurement}
        />
        <FieldList
          query={query}
          onToggleField={this.handleToggleField}
          onGroupByTime={this.handleGroupByTime}
          applyFuncsToField={this.handleApplyFuncsToField}
        />
        <TagList
          query={query}
          onChooseTag={this.handleChooseTag}
          onGroupByTag={this.handleGroupByTag}
          onToggleTagAcceptance={this.handleToggleTagAcceptance}
        />
      </div>
    )
  },
})

export default QueryEditor
