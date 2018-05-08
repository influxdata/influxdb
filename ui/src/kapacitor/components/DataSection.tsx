import React, {PureComponent} from 'react'

import DatabaseList from 'src/shared/components/DatabaseList'
import MeasurementList from 'src/shared/components/MeasurementList'
import FieldList from 'src/shared/components/FieldList'

import {defaultEveryFrequency} from 'src/kapacitor/constants'

import {SourceContext} from 'src/CheckSources'

import {
  ApplyFuncsToFieldArgs,
  Field,
  Namespace,
  QueryConfig,
  Source,
  TimeRange,
  Tag,
} from 'src/types'

interface Actions {
  chooseNamespace: (id: string, namespace: Namespace) => void
  chooseMeasurement: (id: string, measurement: string) => void
  toggleField: (id: string, field: Field) => void
  groupByTime: (id: string, time: string | null) => void
  applyFuncsToField: (id: string, fieldFunc: ApplyFuncsToFieldArgs) => void
  chooseTag: (id: string, tag: Tag) => void
  toggleTagAcceptance: (id: string) => void
  groupByTag: (id: string, tagKey: string) => void
  removeFuncs: (id: string, fields: Field[]) => void
}

interface Props {
  actions: Actions
  query: QueryConfig
  isDeadman: boolean
  isKapacitorRule: boolean
  onAddEvery: () => void
  timeRange: TimeRange
}

class DataSection extends PureComponent<Props> {
  public render() {
    const {query, isDeadman, isKapacitorRule, onAddEvery} = this.props

    return (
      <SourceContext.Consumer>
        {(source: Source) => (
          <div className="rule-section">
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
                isKapacitorRule={isKapacitorRule}
              />
              {isDeadman ? null : (
                <FieldList
                  query={query}
                  applyFuncsToField={this.handleApplyFuncsToField(onAddEvery)}
                  onGroupByTime={this.handleGroupByTime}
                  onToggleField={this.handleToggleField}
                  removeFuncs={this.handleRemoveFuncs}
                  isKapacitorRule={isKapacitorRule}
                  source={source}
                />
              )}
            </div>
          </div>
        )}
      </SourceContext.Consumer>
    )
  }

  private handleChooseNamespace = (namespace: Namespace): void => {
    const {actions, query} = this.props
    actions.chooseNamespace(query.id, namespace)
  }

  private handleChooseMeasurement = (measurement: string): void => {
    const {actions, query} = this.props
    actions.chooseMeasurement(query.id, measurement)
  }

  private handleToggleField = (field: Field): void => {
    const {actions, query} = this.props
    actions.toggleField(query.id, field)
  }

  private handleGroupByTime = (time: string): void => {
    const {actions, query} = this.props
    actions.groupByTime(query.id, time)
  }

  private handleApplyFuncsToField = (onAddEvery: (every: string) => void) => (
    fieldFunc: ApplyFuncsToFieldArgs
  ): void => {
    const {actions, query} = this.props
    actions.applyFuncsToField(query.id, fieldFunc)
    onAddEvery(defaultEveryFrequency)
  }

  private handleChooseTag = (tag: Tag): void => {
    const {actions, query} = this.props
    actions.chooseTag(query.id, tag)
  }

  private handleToggleTagAcceptance = (): void => {
    const {actions, query} = this.props
    actions.toggleTagAcceptance(query.id)
  }

  private handleGroupByTag = (tagKey: string): void => {
    const {actions, query} = this.props
    actions.groupByTag(query.id, tagKey)
  }

  private handleRemoveFuncs = (fields: Field[]): void => {
    const {actions, query} = this.props
    actions.removeFuncs(query.id, fields)
  }
}

export default DataSection
