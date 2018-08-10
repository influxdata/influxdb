import React, {PureComponent} from 'react'
import _ from 'lodash'

import {
  ApplyFuncsToFieldArgs,
  Field,
  FieldFunc,
  GroupBy,
  QueryConfig,
  Source,
  TimeShift,
} from 'src/types'

import QueryOptions from 'src/shared/components/QueryOptions'
import FieldListItem from 'src/data_explorer/components/FieldListItem'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

import {showFieldKeys} from 'src/shared/apis/metaQuery'
import showFieldKeysParser from 'src/shared/parsing/showFieldKeys'
import {
  functionNames,
  numFunctions,
  getFieldsWithName,
  getFuncsByFieldName,
} from 'src/shared/reducers/helpers/fields'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface GroupByOption extends GroupBy {
  menuOption: string
}

interface TimeShiftOption extends TimeShift {
  text: string
}
interface Links {
  proxy: string
}

interface Props {
  query: QueryConfig
  addInitialField?: (field: Field, groupBy: GroupBy) => void
  applyFuncsToField: (field: ApplyFuncsToFieldArgs, groupBy?: GroupBy) => void
  onFill?: (fill: string) => void
  onGroupByTime: (groupByOption: string) => void
  onTimeShift?: (shift: TimeShiftOption) => void
  onToggleField: (field: Field) => void
  removeFuncs: (fields: Field[]) => void
  querySource?: {
    links: Links
  }
  initialGroupByTime?: string | null
  isQuerySupportedByExplorer?: boolean
  source: Source
}

interface State {
  fields: Field[]
}

@ErrorHandling
class FieldList extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    initialGroupByTime: null,
  }

  constructor(props) {
    super(props)
    this.state = {
      fields: [],
    }
  }

  public componentDidMount() {
    const {database, measurement} = this.props.query
    if (!database || !measurement) {
      return
    }

    this.getFields()
  }

  public componentDidUpdate(prevProps) {
    const {querySource, query} = this.props
    const {database, measurement, retentionPolicy} = query
    const {
      database: prevDB,
      measurement: prevMeas,
      retentionPolicy: prevRP,
    } = prevProps.query
    if (!database || !measurement) {
      return
    }

    if (
      database === prevDB &&
      measurement === prevMeas &&
      retentionPolicy === prevRP &&
      _.isEqual(prevProps.querySource, querySource)
    ) {
      return
    }

    this.getFields()
  }

  public render() {
    const {
      query: {database, measurement, fields = [], groupBy, fill, shifts},
      isQuerySupportedByExplorer,
    } = this.props

    const hasAggregates = numFunctions(fields) > 0
    const noDBorMeas = !database || !measurement
    const isDisabled = !isQuerySupportedByExplorer

    return (
      <div className="query-builder--column">
        <div className="query-builder--heading">
          <span>Fields</span>
          {hasAggregates ? (
            <QueryOptions
              fill={fill}
              shift={_.first(shifts)}
              groupBy={groupBy}
              onFill={this.handleFill}
              onTimeShift={this.handleTimeShift}
              onGroupByTime={this.handleGroupByTime}
              isDisabled={isDisabled}
            />
          ) : null}
        </div>
        {noDBorMeas ? (
          <div className="query-builder--list-empty">
            <span>
              No <strong>Measurement</strong> selected
            </span>
          </div>
        ) : (
          <div className="query-builder--list">
            <FancyScrollbar>
              {this.state.fields.map((fieldFunc, i) => {
                const selectedFields = getFieldsWithName(
                  fieldFunc.value,
                  fields
                )

                const funcs: FieldFunc[] = getFuncsByFieldName(
                  fieldFunc.value,
                  fields
                )
                const fieldFuncs = selectedFields.length
                  ? selectedFields
                  : [fieldFunc]

                return (
                  <FieldListItem
                    key={i}
                    onToggleField={this.handleToggleField}
                    onApplyFuncsToField={this.handleApplyFuncs}
                    isSelected={!!selectedFields.length}
                    fieldFuncs={fieldFuncs}
                    funcs={functionNames(funcs)}
                    isDisabled={isDisabled}
                  />
                )
              })}
            </FancyScrollbar>
          </div>
        )}
      </div>
    )
  }

  private handleGroupByTime = (groupBy: GroupByOption): void => {
    this.props.onGroupByTime(groupBy.menuOption)
  }

  private handleFill = (fill: string): void => {
    this.props.onFill(fill)
  }

  private handleToggleField = (field: Field) => {
    const {
      query,
      onToggleField,
      addInitialField,
      initialGroupByTime: time,
      isQuerySupportedByExplorer,
    } = this.props
    const {fields, groupBy} = query
    const isDisabled = !isQuerySupportedByExplorer

    if (isDisabled) {
      return
    }
    const initialGroupBy = {...groupBy, time}

    if (!_.size(fields)) {
      return addInitialField(field, initialGroupBy)
    }

    onToggleField(field)
  }

  private handleApplyFuncs = (fieldFunc: ApplyFuncsToFieldArgs): void => {
    const {
      query,
      removeFuncs,
      applyFuncsToField,
      initialGroupByTime: time,
    } = this.props
    const {groupBy, fields} = query
    const {funcs} = fieldFunc

    // If one field has no funcs, all fields must have no funcs
    if (!_.size(funcs)) {
      return removeFuncs(fields)
    }

    // If there is no groupBy time, set one
    if (!groupBy.time) {
      return applyFuncsToField(fieldFunc, {...groupBy, time})
    }

    applyFuncsToField(fieldFunc, groupBy)
  }

  private handleTimeShift = (shift: TimeShiftOption): void => {
    this.props.onTimeShift(shift)
  }

  private getFields = (): void => {
    const {database, measurement, retentionPolicy} = this.props.query
    const {querySource, source} = this.props

    const proxy =
      _.get(querySource, ['links', 'proxy'], null) || source.links.proxy

    showFieldKeys(proxy, database, measurement, retentionPolicy).then(resp => {
      const {errors, fieldSets} = showFieldKeysParser(resp.data)
      if (errors.length) {
        console.error('Error parsing fields keys: ', errors)
      }

      const newFields = _.get(fieldSets, measurement, []).map(f => ({
        value: f,
        type: 'field',
      }))

      this.setState({
        fields: newFields,
      })
    })
  }
}

export default FieldList
