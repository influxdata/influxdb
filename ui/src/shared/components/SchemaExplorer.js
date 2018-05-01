import React from 'react'
import PropTypes from 'prop-types'

import DatabaseList from 'src/shared/components/DatabaseList'
import MeasurementList from 'src/shared/components/MeasurementList'
import FieldList from 'src/shared/components/FieldList'

const actionBinder = (id, action) => (...args) => action(id, ...args)

const SchemaExplorer = ({
  query,
  source,
  initialGroupByTime,
  actions: {
    fill,
    timeShift,
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
  isQuerySupportedByExplorer = true,
}) => {
  const {id} = query

  return (
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
        isQuerySupportedByExplorer={isQuerySupportedByExplorer}
      />
      <FieldList
        source={source}
        query={query}
        querySource={source}
        onFill={actionBinder(id, fill)}
        initialGroupByTime={initialGroupByTime}
        onTimeShift={actionBinder(id, timeShift)}
        removeFuncs={actionBinder(id, removeFuncs)}
        onToggleField={actionBinder(id, toggleField)}
        onGroupByTime={actionBinder(id, groupByTime)}
        addInitialField={actionBinder(id, addInitialField)}
        applyFuncsToField={actionBinder(id, applyFuncsToField)}
        isQuerySupportedByExplorer={isQuerySupportedByExplorer}
      />
    </div>
  )
}

const {bool, func, shape, string} = PropTypes

SchemaExplorer.defaultProps = {
  isQuerySupportedByExplorer: true,
}

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
  isQuerySupportedByExplorer: bool,
}

export default SchemaExplorer
