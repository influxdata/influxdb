import React, {PropTypes, Component} from 'react'
import _ from 'lodash'

import FieldListItem from 'src/data_explorer/components/FieldListItem'
import GroupByTimeDropdown from 'src/data_explorer/components/GroupByTimeDropdown'
import FillQuery from 'shared/components/FillQuery'
import FancyScrollbar from 'shared/components/FancyScrollbar'

import {showFieldKeys} from 'shared/apis/metaQuery'
import showFieldKeysParser from 'shared/parsing/showFieldKeys'
import {
  functionNames,
  numFunctions,
  getFieldsWithName,
  getFuncsByFieldName,
} from 'shared/reducers/helpers/fields'

class FieldList extends Component {
  constructor(props) {
    super(props)
    this.state = {
      fields: [],
    }
  }

  componentDidMount() {
    const {database, measurement} = this.props.query
    if (!database || !measurement) {
      return
    }

    this._getFields()
  }

  componentDidUpdate(prevProps) {
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

  handleGroupByTime = groupBy => {
    this.props.onGroupByTime(groupBy.menuOption)
  }

  handleFill = fill => {
    this.props.onFill(fill)
  }

  handleApplyFuncs = fieldFunc => {
    const {removeFuncs, query, applyFuncsToField} = this.props
    const {fields, groupBy} = query
    const {funcs} = fieldFunc

    // If one field has no funcs, all fields must have no funcs
    if (!_.size(funcs)) {
      return removeFuncs(fields, groupBy)
    }

    console.log('queryfields: funcs: ', fields, funcs)

    applyFuncsToField(fieldFunc)
  }

  _getFields = () => {
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

      this.setState({
        fields: fieldSets[measurement].map(f => ({name: f, type: 'field'})),
      })
    })
  }

  render() {
    const {
      query: {database, measurement, fields = [], groupBy, fill},
      isKapacitorRule,
      isInDataExplorer,
      onToggleField,
    } = this.props

    const hasAggregates = numFunctions(fields) > 0
    const hasGroupByTime = groupBy.time
    const noDBorMeas = !database || !measurement

    return (
      <div className="query-builder--column">
        <div className="query-builder--heading">
          <span>Fields</span>
          {hasAggregates
            ? <div className="query-builder--groupby-fill-container">
                <GroupByTimeDropdown
                  isOpen={!hasGroupByTime}
                  selected={groupBy.time}
                  onChooseGroupByTime={this.handleGroupByTime}
                  isInRuleBuilder={isKapacitorRule}
                  isInDataExplorer={isInDataExplorer}
                />
                {isKapacitorRule
                  ? null
                  : <FillQuery value={fill} onChooseFill={this.handleFill} />}
              </div>
            : null}
        </div>
        {noDBorMeas
          ? <div className="query-builder--list-empty">
              <span>
                No <strong>Measurement</strong> selected
              </span>
            </div>
          : <div className="query-builder--list">
              <FancyScrollbar>
                {this.state.fields.map((fieldFunc, i) => {
                  const selectedFields = getFieldsWithName(
                    fieldFunc.name,
                    fields
                  )

                  const funcs = getFuncsByFieldName(fieldFunc.name, fields)

                  return (
                    <FieldListItem
                      key={i}
                      onToggleField={onToggleField}
                      onApplyFuncsToField={this.handleApplyFuncs}
                      isSelected={!!selectedFields.length}
                      fieldFuncs={
                        selectedFields.length ? selectedFields : [fieldFunc]
                      }
                      funcs={functionNames(funcs)}
                      isKapacitorRule={isKapacitorRule}
                    />
                  )
                })}
              </FancyScrollbar>
            </div>}
      </div>
    )
  }
}

const {bool, func, shape, string} = PropTypes

FieldList.defaultProps = {
  isKapacitorRule: false,
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
  }).isRequired,
  onToggleField: func.isRequired,
  onGroupByTime: func.isRequired,
  onFill: func,
  applyFuncsToField: func.isRequired,
  isKapacitorRule: bool,
  isInDataExplorer: bool,
  querySource: shape({
    links: shape({
      proxy: string.isRequired,
    }).isRequired,
  }),
  removeFuncs: func.isRequired,
}

export default FieldList
