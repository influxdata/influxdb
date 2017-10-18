import React, {PropTypes} from 'react'

import DatabaseList from 'src/shared/components/DatabaseList'
import MeasurementList from 'src/shared/components/MeasurementList'
import FieldList from 'src/shared/components/FieldList'

const actionBinder = (id, action) => (...args) => action(id, ...args)

const SchemaExplorer = ({
  query,
  query: {id},
  source,
  initialGroupByTime,
  actions: {
    fill,
    chooseTag,
    groupByTag,
    groupByTime,
    toggleField,
    removeFuncs,
    addInitialField,
    chooseNamespace,
    chooseMeasurement,
    applyFuncsToField,
    toggleTagAcceptance,
  },
}) =>
  <div className="query-builder">
    <DatabaseList
      query={query}
      querySource={source}
      onChooseNamespace={actionBinder(id, chooseNamespace)}
    />
    <MeasurementList
      source={source}
      query={query}
      querySource={source}
      onChooseTag={actionBinder(id, chooseTag)}
      onGroupByTag={actionBinder(id, groupByTag)}
      onChooseMeasurement={actionBinder(id, chooseMeasurement)}
      onToggleTagAcceptance={actionBinder(id, toggleTagAcceptance)}
    />
    <FieldList
      source={source}
      query={query}
      querySource={source}
      initialGroupByTime={initialGroupByTime}
      onToggleField={actionBinder(id, toggleField)}
      onFill={actionBinder(id, fill)}
      onGroupByTime={actionBinder(id, groupByTime)}
      applyFuncsToField={actionBinder(id, applyFuncsToField)}
      removeFuncs={actionBinder(id, removeFuncs)}
      addInitialField={actionBinder(id, addInitialField)}
    />
  </div>

const {func, shape, string} = PropTypes

SchemaExplorer.propTypes = {
  query: shape({
    id: string,
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
    fill: func.isRequired,
    editRawTextAsync: func.isRequired,
    addInitialField: func.isRequired,
    removeFuncs: func.isRequired,
  }).isRequired,
  source: shape({}),
  initialGroupByTime: string.isRequired,
}

export default SchemaExplorer
