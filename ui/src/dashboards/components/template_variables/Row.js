import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import uniq from 'lodash/uniq'

import OnClickOutside from 'react-onclickoutside'
import classnames from 'classnames'

import Dropdown from 'shared/components/Dropdown'
import TemplateQueryBuilder from 'src/dashboards/components/template_variables/TemplateQueryBuilder'
import TableInput from 'src/dashboards/components/template_variables/TableInput'
import RowValues from 'src/dashboards/components/template_variables/RowValues'
import RowButtons from 'src/dashboards/components/template_variables/RowButtons'

import {runTemplateVariableQuery as runTemplateVariableQueryAJAX} from 'src/dashboards/apis'

import parsers from 'shared/parsing'

import {TEMPLATE_TYPES} from 'src/dashboards/constants'
import generateTemplateVariableQuery from 'src/dashboards/utils/templateVariableQueryGenerator'

import {errorThrown as errorThrownAction} from 'shared/actions/errors'
import {publishAutoDismissingNotification} from 'shared/dispatchers'

const compact = values => uniq(values).filter(value => /\S/.test(value))

const TemplateVariableRow = ({
  template: {id, tempVar, values},
  isEditing,
  selectedType,
  selectedDatabase,
  selectedMeasurement,
  onSelectType,
  onSelectDatabase,
  onSelectMeasurement,
  selectedTagKey,
  onSelectTagKey,
  onStartEdit,
  onCancelEdit,
  autoFocusTarget,
  onSubmit,
  onErrorThrown,
  onDeleteTempVar,
}) =>
  <form
    className={classnames('template-variable-manager--table-row', {
      editing: isEditing,
    })}
    onSubmit={onSubmit({
      selectedType,
      selectedDatabase,
      selectedMeasurement,
      selectedTagKey,
    })}
  >
    <div className="tvm--col-1">
      <TableInput
        name="tempVar"
        defaultValue={tempVar}
        isEditing={isEditing}
        onStartEdit={onStartEdit}
        autoFocusTarget={autoFocusTarget}
      />
    </div>
    <div className="tvm--col-2">
      <Dropdown
        items={TEMPLATE_TYPES}
        onChoose={onSelectType}
        onClick={onStartEdit}
        selected={TEMPLATE_TYPES.find(t => t.type === selectedType).text}
        className="dropdown-140"
      />
    </div>
    <div className="tvm--col-3">
      <TemplateQueryBuilder
        onSelectDatabase={onSelectDatabase}
        selectedType={selectedType}
        selectedDatabase={selectedDatabase}
        onSelectMeasurement={onSelectMeasurement}
        selectedMeasurement={selectedMeasurement}
        selectedTagKey={selectedTagKey}
        onSelectTagKey={onSelectTagKey}
        onStartEdit={onStartEdit}
        onErrorThrown={onErrorThrown}
      />
      <RowValues
        selectedType={selectedType}
        values={values}
        isEditing={isEditing}
        onStartEdit={onStartEdit}
        autoFocusTarget={autoFocusTarget}
      />
    </div>
    <div className="tvm--col-4">
      <RowButtons
        onStartEdit={onStartEdit}
        isEditing={isEditing}
        onCancelEdit={onCancelEdit}
        onDelete={onDeleteTempVar}
        id={id}
        selectedType={selectedType}
      />
    </div>
  </form>

class RowWrapper extends Component {
  constructor(props) {
    super(props)
    const {template: {type, query, isNew}} = this.props

    this.state = {
      isEditing: !!isNew,
      isNew: !!isNew,
      hasBeenSavedToComponentStateOnce: !isNew,
      selectedType: type,
      selectedDatabase: query && query.db,
      selectedMeasurement: query && query.measurement,
      selectedTagKey: query && query.tagKey,
      autoFocusTarget: 'tempVar',
    }

    this.runTemplateVariableQuery = ::this.runTemplateVariableQuery
  }

  handleSubmit = ({
    selectedDatabase: database,
    selectedMeasurement: measurement,
    selectedTagKey: tagKey,
    selectedType: type,
  }) => async e => {
    e.preventDefault()

    const {
      source,
      template,
      template: {id},
      onRunQuerySuccess,
      onRunQueryFailure,
      tempVarAlreadyExists,
      notify,
    } = this.props

    const _tempVar = e.target.tempVar.value.replace(/\u003a/g, '')
    const tempVar = `\u003a${_tempVar}\u003a` // add ':'s

    if (tempVarAlreadyExists(tempVar, id)) {
      return notify(
        'error',
        `Variable '${_tempVar}' already exists. Please enter a new value.`
      )
    }

    this.setState({
      isEditing: false,
      hasBeenSavedToComponentStateOnce: true,
    })

    const {query, tempVars} = generateTemplateVariableQuery({
      type,
      tempVar,
      query: {
        database,
        // rp, TODO
        measurement,
        tagKey,
      },
    })

    const queryConfig = {
      type,
      tempVars,
      query,
      database,
      // rp: TODO
      measurement,
      tagKey,
    }

    try {
      let parsedData
      if (type === 'csv') {
        parsedData = e.target.values.value.split(',').map(value => value.trim())
      } else {
        parsedData = await this.runTemplateVariableQuery(source, queryConfig)
      }

      onRunQuerySuccess(template, queryConfig, compact(parsedData), tempVar)
    } catch (error) {
      onRunQueryFailure(error)
    }
  }

  handleClickOutside() {
    this.handleCancelEdit()
  }

  handleStartEdit = name => () => {
    this.setState({isEditing: true, autoFocusTarget: name})
  }

  handleCancelEdit = () => {
    const {
      template: {type, query: {db, measurement, tagKey}, id},
      onDelete,
    } = this.props
    const {hasBeenSavedToComponentStateOnce} = this.state

    if (!hasBeenSavedToComponentStateOnce) {
      return onDelete(id)
    }
    this.setState({
      selectedType: type,
      selectedDatabase: db,
      selectedMeasurement: measurement,
      selectedTagKey: tagKey,
      isEditing: false,
    })
  }

  handleSelectType = item => {
    this.setState({
      selectedType: item.type,
      selectedDatabase: null,
      selectedMeasurement: null,
      selectedTagKey: null,
    })
  }

  handleSelectDatabase = item => {
    this.setState({selectedDatabase: item.text})
  }

  handleSelectMeasurement = item => {
    this.setState({selectedMeasurement: item.text})
  }

  handleSelectTagKey = item => {
    this.setState({selectedTagKey: item.text})
  }

  runTemplateVariableQuery = async (
    source,
    {query, database, rp, tempVars, type, measurement, tagKey}
  ) => {
    try {
      const {data} = await runTemplateVariableQueryAJAX(source, {
        query,
        db: database,
        rp,
        tempVars,
      })
      const parsedData = parsers[type](data, tagKey || measurement) // tagKey covers tagKey and fieldKey
      if (parsedData.errors.length) {
        throw parsedData.errors
      }

      return parsedData[type]
    } catch (error) {
      console.error(error)
      throw error
    }
  }

  handleDelete = id => () => {
    this.props.onDelete(id)
  }

  render() {
    const {
      isEditing,
      selectedType,
      selectedDatabase,
      selectedMeasurement,
      selectedTagKey,
      autoFocusTarget,
    } = this.state

    return (
      <TemplateVariableRow
        {...this.props}
        isEditing={isEditing}
        selectedType={selectedType}
        selectedDatabase={selectedDatabase}
        selectedMeasurement={selectedMeasurement}
        selectedTagKey={selectedTagKey}
        onSelectType={this.handleSelectType}
        onSelectDatabase={this.handleSelectDatabase}
        onSelectMeasurement={this.handleSelectMeasurement}
        onSelectTagKey={this.handleSelectTagKey}
        onStartEdit={this.handleStartEdit}
        onCancelEdit={this.handleCancelEdit}
        autoFocusTarget={autoFocusTarget}
        onSubmit={this.handleSubmit}
        onDeleteTempVar={this.handleDelete}
      />
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

RowWrapper.propTypes = {
  source: shape({
    links: shape({
      proxy: string,
    }),
  }).isRequired,
  template: shape({
    type: string.isRequired,
    tempVar: string.isRequired,
    query: shape({
      db: string,
      influxql: string,
      measurement: string,
      tagKey: string,
    }),
    values: arrayOf(
      shape({
        value: string.isRequired,
        type: string.isRequired,
        selected: bool.isRequired,
      })
    ).isRequired,
    links: shape({
      self: string.isRequired,
    }),
  }),
  onRunQuerySuccess: func.isRequired,
  onRunQueryFailure: func.isRequired,
  onDelete: func.isRequired,
  tempVarAlreadyExists: func.isRequired,
  notify: func.isRequired,
}

TemplateVariableRow.propTypes = {
  ...RowWrapper.propTypes,
  selectedType: string.isRequired,
  selectedDatabase: string,
  selectedTagKey: string,
  onSelectType: func.isRequired,
  onSelectDatabase: func.isRequired,
  onSelectTagKey: func.isRequired,
  onStartEdit: func.isRequired,
  onCancelEdit: func.isRequired,
  onSubmit: func.isRequired,
  onErrorThrown: func.isRequired,
}

const mapDispatchToProps = dispatch => ({
  onErrorThrown: bindActionCreators(errorThrownAction, dispatch),
  notify: bindActionCreators(publishAutoDismissingNotification, dispatch),
})

export default connect(null, mapDispatchToProps)(OnClickOutside(RowWrapper))
