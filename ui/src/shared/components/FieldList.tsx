import React, {PureComponent} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'

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

@ErrorHandling
class FieldList extends PureComponent {
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

    this._getFields()
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

    this._getFields()
  }

  public handleGroupByTime = groupBy => {
    this.props.onGroupByTime(groupBy.menuOption)
  }

  public handleFill = fill => {
    this.props.onFill(fill)
  }

  public handleToggleField = field => {
    const {
      query,
      onToggleField,
      addInitialField,
      initialGroupByTime: time,
      isKapacitorRule,
      isQuerySupportedByExplorer,
    } = this.props
    const {fields, groupBy} = query
    if (!isQuerySupportedByExplorer) {
      return
    }
    const initialGroupBy = {...groupBy, time}

    if (!_.size(fields)) {
      return isKapacitorRule
        ? onToggleField(field)
        : addInitialField(field, initialGroupBy)
    }

    onToggleField(field)
  }

  public handleApplyFuncs = fieldFunc => {
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

  public handleTimeShift = shift => {
    this.props.onTimeShift(shift)
  }

  public _getFields = () => {
    const {database, measurement, retentionPolicy} = this.props.query
    const {source} = this.context
    const {querySource} = this.props

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

  public render() {
    const {
      query: {database, measurement, fields = [], groupBy, fill, shifts},
      isQuerySupportedByExplorer,
      isKapacitorRule,
    } = this.props

    const hasAggregates = numFunctions(fields) > 0
    const noDBorMeas = !database || !measurement

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
              isKapacitorRule={isKapacitorRule}
              onTimeShift={this.handleTimeShift}
              onGroupByTime={this.handleGroupByTime}
              isDisabled={!isQuerySupportedByExplorer}
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

                const funcs = getFuncsByFieldName(fieldFunc.value, fields)
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
                    isKapacitorRule={isKapacitorRule}
                    isDisabled={!isQuerySupportedByExplorer}
                  />
                )
              })}
            </FancyScrollbar>
          </div>
        )}
      </div>
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

FieldList.defaultProps = {
  isKapacitorRule: false,
  initialGroupByTime: null,
}

FieldList.contextTypes = {
  source: shape({
    links: shape({
      proxy: string.isRequired,
    }).isRequired,
  }).isRequired,
}

FieldList.propTypes = {
  query: shape({
    database: string,
    retentionPolicy: string,
    measurement: string,
    shifts: arrayOf(
      shape({
        label: string,
        unit: string,
        quantity: string,
      })
    ),
  }).isRequired,
  onTimeShift: func,
  onToggleField: func.isRequired,
  onGroupByTime: func.isRequired,
  onFill: func,
  applyFuncsToField: func.isRequired,
  isKapacitorRule: bool,
  querySource: shape({
    links: shape({
      proxy: string.isRequired,
    }).isRequired,
  }),
  removeFuncs: func.isRequired,
  addInitialField: func,
  initialGroupByTime: string,
  isQuerySupportedByExplorer: bool,
}

export default FieldList
