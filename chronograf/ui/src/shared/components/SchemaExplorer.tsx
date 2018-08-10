import React, {PureComponent} from 'react'

import DatabaseList from 'src/shared/components/DatabaseList'
import MeasurementList from 'src/shared/components/MeasurementList'
import FieldList from 'src/shared/components/FieldList'

import {QueryConfig, Source} from 'src/types'

const actionBinder = (id, action) => (...args) => action(id, ...args)

interface Props {
  query: QueryConfig
  source: Source
  actions: any
  initialGroupByTime: string
  isQuerySupportedByExplorer?: boolean
}

class SchemaExplorer extends PureComponent<Props> {
  public render() {
    const {
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
    } = this.props
    const {id} = query

    return (
      <div className="query-builder">
        <DatabaseList
          query={query}
          source={source}
          onChooseNamespace={actionBinder(id, chooseNamespace)}
        />
        <MeasurementList
          source={source}
          query={query}
          onChooseTag={actionBinder(id, chooseTag)}
          onGroupByTag={actionBinder(id, groupByTag)}
          onChooseMeasurement={actionBinder(id, chooseMeasurement)}
          onToggleTagAcceptance={actionBinder(id, toggleTagAcceptance)}
          isQuerySupportedByExplorer={isQuerySupportedByExplorer}
        />
        <FieldList
          source={source}
          query={query}
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
}

export default SchemaExplorer
