import React, {PropTypes, Component} from 'react'

import DatabaseList from 'src/shared/components/DatabaseList'
import MeasurementList from 'src/shared/components/MeasurementList'
import FieldList from 'src/shared/components/FieldList'

import {defaultEveryFrequency} from 'src/kapacitor/constants'

class DataSection extends Component {
  constructor(props) {
    super(props)
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
    const {query, isKapacitorRule, isDeadman} = this.props

    return (
      <div className="rule-section">
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
          {isDeadman
            ? null
            : <FieldList
                query={query}
                onToggleField={this.handleToggleField}
                onGroupByTime={this.handleGroupByTime}
                applyFuncsToField={this.handleApplyFuncsToField}
                isKapacitorRule={isKapacitorRule}
              />}
        </div>
      </div>
    )
  }
}

const {bool, func, shape, string} = PropTypes

DataSection.propTypes = {
  query: shape({
    id: string.isRequired,
  }).isRequired,
  addFlashMessage: func,
  actions: shape({
    chooseNamespace: func.isRequired,
    chooseMeasurement: func.isRequired,
    applyFuncsToField: func.isRequired,
    chooseTag: func.isRequired,
    groupByTag: func.isRequired,
    toggleField: func.isRequired,
    groupByTime: func.isRequired,
    toggleTagAcceptance: func.isRequired,
  }).isRequired,
  onAddEvery: func.isRequired,
  onRemoveEvery: func.isRequired,
  timeRange: shape({}).isRequired,
  isKapacitorRule: bool,
  isDeadman: bool,
}

export default DataSection
