import React, {PropTypes} from 'react'
import buildInfluxQLQuery from 'utils/influxql'
import classnames from 'classnames'

import DatabaseList from 'src/shared/components/DatabaseList'
import MeasurementList from 'src/shared/components/MeasurementList'
import FieldList from 'src/shared/components/FieldList'
import FillQuery from 'shared/components/FillQuery'

import {defaultEveryFrequency} from 'src/kapacitor/constants'

export const DataSection = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        kapacitors: PropTypes.string.isRequired,
      }).isRequired,
    }),
    query: PropTypes.shape({
      id: PropTypes.string.isRequired,
    }).isRequired,
    addFlashMessage: PropTypes.func,
    actions: PropTypes.shape({
      chooseNamespace: PropTypes.func.isRequired,
      chooseMeasurement: PropTypes.func.isRequired,
      applyFuncsToField: PropTypes.func.isRequired,
      chooseTag: PropTypes.func.isRequired,
      groupByTag: PropTypes.func.isRequired,
      toggleField: PropTypes.func.isRequired,
      groupByTime: PropTypes.func.isRequired,
      toggleTagAcceptance: PropTypes.func.isRequired,
    }).isRequired,
    onAddEvery: PropTypes.func.isRequired,
    onRemoveEvery: PropTypes.func.isRequired,
    timeRange: PropTypes.shape({}).isRequired,
    isKapacitorRule: PropTypes.bool,
  },

  childContextTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
        self: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getChildContext() {
    return {source: this.props.source}
  },

  handleChooseNamespace(namespace) {
    this.props.actions.chooseNamespace(this.props.query.id, namespace)
  },

  handleChooseMeasurement(measurement) {
    this.props.actions.chooseMeasurement(this.props.query.id, measurement)
  },

  handleToggleField(field) {
    this.props.actions.toggleField(this.props.query.id, field)
    // Every is only added when a function has been added to a field.
    // Here, the field is selected without a function.
    this.props.onRemoveEvery()
    // Because there are no functions there is no group by time.
    this.props.actions.groupByTime(this.props.query.id, null)
  },

  handleGroupByTime(time) {
    this.props.actions.groupByTime(this.props.query.id, time)
  },

  handleApplyFuncsToField(fieldFunc) {
    this.props.actions.applyFuncsToField(this.props.query.id, fieldFunc)
    this.props.onAddEvery(defaultEveryFrequency)
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

  render() {
    const {query, timeRange: {lower}} = this.props
    const statement = query.rawText || buildInfluxQLQuery({lower}, query)

    return (
      <div className="rule-section">
        <h3 className="rule-section--heading">
          Select a Time Series
          <FillQuery
            onSelection={e => {
              console.log(e)
            }}
            theme="green"
            size="sm"
          />
        </h3>
        <div className="rule-section--body">
          <pre className="rule-section--border-bottom">
            <code
              className={classnames({
                'metric-placeholder': !statement,
              })}
            >
              {statement || 'Build a query below'}
            </code>
          </pre>
          {this.renderQueryBuilder()}
        </div>
      </div>
    )
  },

  renderQueryBuilder() {
    const {query, isKapacitorRule} = this.props

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
          onGroupByTag={this.handleGroupByTag}
          onToggleTagAcceptance={this.handleToggleTagAcceptance}
        />
        <FieldList
          query={query}
          onToggleField={this.handleToggleField}
          onGroupByTime={this.handleGroupByTime}
          applyFuncsToField={this.handleApplyFuncsToField}
          isKapacitorRule={isKapacitorRule}
        />
      </div>
    )
  },
})

export default DataSection
