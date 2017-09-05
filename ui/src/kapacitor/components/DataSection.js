import React, {PropTypes, Component} from 'react'

import DatabaseList from 'src/shared/components/DatabaseList'
import MeasurementList from 'src/shared/components/MeasurementList'
import FieldList from 'src/shared/components/FieldList'

import {defaultEveryFrequency} from 'src/kapacitor/constants'

class DataSection extends Component {
  constructor(props) {
    super(props)
  }

  getChildContext() {
    return {source: this.props.source}
  }

  handleChooseNamespace = namespace => {
    this.props.actions.chooseNamespace(this.props.query.id, namespace)
  }

  handleChooseMeasurement = measurement => {
    this.props.actions.chooseMeasurement(this.props.query.id, measurement)
  }

  handleToggleField = field => {
    this.props.actions.toggleField(this.props.query.id, field)
    // Every is only added when a function has been added to a field.
    // Here, the field is selected without a function.
    this.props.onRemoveEvery()
    // Because there are no functions there is no group by time.
    this.props.actions.groupByTime(this.props.query.id, null)
  }

  handleGroupByTime = time => {
    this.props.actions.groupByTime(this.props.query.id, time)
  }

  handleApplyFuncsToField = fieldFunc => {
    this.props.actions.applyFuncsToField(this.props.query.id, fieldFunc)
    this.props.onAddEvery(defaultEveryFrequency)
  }

  handleChooseTag = tag => {
    this.props.actions.chooseTag(this.props.query.id, tag)
  }

  handleToggleTagAcceptance = () => {
    this.props.actions.toggleTagAcceptance(this.props.query.id)
  }

  handleGroupByTag = tagKey => {
    this.props.actions.groupByTag(this.props.query.id, tagKey)
  }

  render() {
    const {query, isKapacitorRule} = this.props
    return (
      <div className="rule-section">
        <h3 className="rule-section--heading">Select a Time Series</h3>
        <div className="rule-section--body">
          <div className="query-builder rule-section--border-bottom">
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
        </div>
      </div>
    )
  }
}

DataSection.propTypes = {
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
}

DataSection.childContextTypes = {
  source: PropTypes.shape({
    links: PropTypes.shape({
      proxy: PropTypes.string.isRequired,
      self: PropTypes.string.isRequired,
    }).isRequired,
  }).isRequired,
}

export default DataSection
