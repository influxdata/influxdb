import React, {PropTypes} from 'react'

import DatabaseList from 'src/shared/components/DatabaseList'
import MeasurementList from 'src/shared/components/MeasurementList'
import FieldList from 'src/shared/components/FieldList'

import {defaultEveryFrequency} from 'src/kapacitor/constants'

const makeQueryHandlers = (actions, query) => ({
  handleChooseNamespace: namespace => {
    actions.chooseNamespace(query.id, namespace)
  },

  handleChooseMeasurement: measurement => {
    actions.chooseMeasurement(query.id, measurement)
  },

  handleToggleField: onRemoveEvery => field => {
    actions.toggleField(query.id, field)
    // Every is only added when a function has been added to a field.
    // Here, the field is selected without a function.
    onRemoveEvery()
    // Because there are no functions there is no group by time.
    actions.groupByTime(query.id, null)
  },

  handleGroupByTime: time => {
    actions.groupByTime(query.id, time)
  },

  handleApplyFuncsToField: onAddEvery => fieldFunc => {
    actions.applyFuncsToField(query.id, fieldFunc)
    onAddEvery(defaultEveryFrequency)
  },

  handleChooseTag: tag => {
    actions.chooseTag(query.id, tag)
  },

  handleToggleTagAcceptance: () => {
    actions.toggleTagAcceptance(query.id)
  },

  handleGroupByTag: tagKey => {
    actions.groupByTag(query.id, tagKey)
  },
})

const DataSection = ({
  actions,
  query,
  isDeadman,
  isKapacitorRule,
  onRemoveEvery,
  onAddEvery,
}) => {
  const {
    handleChooseNamespace,
    handleChooseMeasurement,
    handleToggleField,
    handleGroupByTime,
    handleApplyFuncsToField,
    handleChooseTag,
    handleToggleTagAcceptance,
    handleGroupByTag,
  } = makeQueryHandlers(actions, query)

  return (
    <div className="rule-section">
      <div className="query-builder rule-section--border-bottom">
        <DatabaseList query={query} onChooseNamespace={handleChooseNamespace} />
        <MeasurementList
          query={query}
          onChooseMeasurement={handleChooseMeasurement}
          onChooseTag={handleChooseTag}
          onGroupByTag={handleGroupByTag}
          onToggleTagAcceptance={handleToggleTagAcceptance}
        />
        {isDeadman
          ? null
          : <FieldList
              query={query}
              onToggleField={handleToggleField(onRemoveEvery)}
              onGroupByTime={handleGroupByTime}
              applyFuncsToField={handleApplyFuncsToField(onAddEvery)}
              isKapacitorRule={isKapacitorRule}
            />}
      </div>
    </div>
  )
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
