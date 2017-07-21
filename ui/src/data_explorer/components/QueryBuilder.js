import React, {PropTypes} from 'react'

import DatabaseList from './DatabaseList'
import DatabaseDropdown from 'shared/components/DatabaseDropdown'
import MeasurementList from './MeasurementList'
import FieldList from './FieldList'
import QueryEditor from './QueryEditor'
import buildInfluxQLQuery from 'utils/influxql'

const {arrayOf, bool, func, shape, string} = PropTypes

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
    isInDataExplorer: bool,
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
    layout: string,
  },

  handleChooseNamespace(namespace) {
    this.props.actions.chooseNamespace(this.props.query.id, namespace)
  },

  handleChooseMeasurement(measurement) {
    this.props.actions.chooseMeasurement(this.props.query.id, measurement)
  },

  handleToggleField(field) {
    this.props.actions.toggleFieldWithGroupByInterval(
      this.props.query.id,
      field
    )
  },

  handleGroupByTime(time) {
    this.props.actions.groupByTime(this.props.query.id, time)
  },

  handleApplyFuncsToField(fieldFunc) {
    this.props.actions.applyFuncsToField(
      this.props.query.id,
      fieldFunc,
      this.props.isInDataExplorer
    )
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
    const {query, templates, isInDataExplorer} = this.props

    // DE does not understand templating. :dashboardTime: is specific to dashboards
    let timeRange

    if (isInDataExplorer) {
      timeRange = this.props.timeRange
    } else {
      timeRange = query.range || {upper: null, lower: ':dashboardTime:'}
    }

    const q = query.rawText || buildInfluxQLQuery(timeRange, query) || ''

    return (
      <div className="query-maker--tab-contents">
        <QueryEditor
          query={q}
          config={query}
          onUpdate={this.handleEditRawText}
          templates={templates}
          isInDataExplorer={isInDataExplorer}
        />
        {this.renderLists()}
      </div>
    )
  },

  renderLists() {
    const {query, layout, isInDataExplorer} = this.props

    // Panel layout uses a dropdown instead of a list for database selection
    // Also groups measurements & fields into their own container so they
    // can be stacked vertically.
    // TODO: Styles to make all this look proper
    if (layout === 'panel') {
      return (
        <div className="query-builder--panel">
          <DatabaseDropdown
            query={query}
            onChooseNamespace={this.handleChooseNamespace}
          />
          <div className="query-builder">
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
              isInDataExplorer={isInDataExplorer}
            />
          </div>
        </div>
      )
    }

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
          isInDataExplorer={isInDataExplorer}
        />
      </div>
    )
  },
})

export default QueryBuilder
