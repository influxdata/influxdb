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

  handleToggleField: field => {
    actions.toggleField(query.id, field)
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

  handleRemoveFuncs: fields => {
    actions.removeFuncs(query.id, fields)
  },
})

const DataSection = ({
  actions,
  query,
  isDeadman,
  isKapacitorRule,
  onAddEvery,
}) => {
  const {
    handleChooseTag,
    handleGroupByTag,
    handleToggleField,
    handleGroupByTime,
    handleRemoveFuncs,
    handleChooseNamespace,
    handleApplyFuncsToField,
    handleChooseMeasurement,
    handleToggleTagAcceptance,
  } = makeQueryHandlers(actions, query)

  return (
    <div className="rule-section">
      <div className="query-builder">
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
              onToggleField={handleToggleField}
              isKapacitorRule={isKapacitorRule}
              onGroupByTime={handleGroupByTime}
              removeFuncs={handleRemoveFuncs}
              applyFuncsToField={handleApplyFuncsToField(onAddEvery)}
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
  timeRange: shape({}).isRequired,
  isKapacitorRule: bool,
  isDeadman: bool,
}

export default DataSection
