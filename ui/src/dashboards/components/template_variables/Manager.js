import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'
import uuid from 'node-uuid'

import TemplateVariableTable from 'src/dashboards/components/template_variables/Table'

import {TEMPLATE_VARIABLE_TYPES} from 'src/dashboards/constants'

const TemplateVariableManager = ({
  source,
  onClose,
  onDelete,
  isEdited,
  templates,
  onAddVariable,
  onRunQuerySuccess,
  onRunQueryFailure,
  tempVarAlreadyExists,
  onSaveTemplatesSuccess,
  onEditTemplateVariables,
}) =>
  <div className="template-variable-manager">
    <div className="template-variable-manager--header">
      <div className="page-header__left">
        <h1 className="page-header__title">Template Variables</h1>
      </div>
      <div className="page-header__right">
        <button
          className="btn btn-primary btn-sm"
          type="button"
          onClick={onAddVariable}
        >
          Add Variable
        </button>
        <button
          className={classnames('btn btn-success btn-sm', {
            disabled: !isEdited,
          })}
          type="button"
          onClick={onEditTemplateVariables(templates, onSaveTemplatesSuccess)}
        >
          Save Changes
        </button>
        <span className="page-header__dismiss" onClick={onClose(isEdited)} />
      </div>
    </div>
    <div className="template-variable-manager--body">
      <TemplateVariableTable
        source={source}
        templates={templates}
        onRunQuerySuccess={onRunQuerySuccess}
        onRunQueryFailure={onRunQueryFailure}
        onDelete={onDelete}
        tempVarAlreadyExists={tempVarAlreadyExists}
      />
    </div>
  </div>

class TemplateVariableManagerWrapper extends Component {
  constructor(props) {
    super(props)

    this.state = {
      rows: this.props.templates,
      isEdited: false,
    }

    this.onRunQuerySuccess = ::this.onRunQuerySuccess
    this.onSaveTemplatesSuccess = ::this.onSaveTemplatesSuccess
    this.onAddVariable = ::this.onAddVariable
    this.onDeleteTemplateVariable = ::this.onDeleteTemplateVariable
    this.tempVarAlreadyExists = ::this.tempVarAlreadyExists
  }

  onAddVariable() {
    const {rows} = this.state

    const newRow = {
      tempVar: '',
      values: [],
      id: uuid.v4(),
      type: 'csv',
      query: {
        influxql: '',
        db: '',
        // rp, TODO
        measurement: '',
        tagKey: '',
      },
      isNew: true,
    }

    const newRows = [newRow, ...rows]

    this.setState({rows: newRows})
  }

  onRunQuerySuccess(template, queryConfig, parsedData, tempVar) {
    const {rows} = this.state
    const {id, links} = template
    const {
      type,
      query: influxql,
      database: db,
      measurement,
      tagKey,
    } = queryConfig

    // Determine which is the selectedValue, if any
    const currentRow = rows.find(row => row.id === id)

    let selectedValue
    if (currentRow && currentRow.values && currentRow.values.length) {
      const matchedValue = currentRow.values.find(val => val.selected)
      if (matchedValue) {
        selectedValue = matchedValue.value
      }
    }

    if (
      !selectedValue &&
      currentRow &&
      currentRow.values &&
      currentRow.values.length
    ) {
      selectedValue = currentRow.values[0].value
    }

    if (!selectedValue) {
      selectedValue = parsedData[0]
    }

    const values = parsedData.map(value => ({
      value,
      type: TEMPLATE_VARIABLE_TYPES[type],
      selected: selectedValue === value,
    }))

    const templateVariable = {
      tempVar,
      values,
      id,
      type,
      query: {
        influxql,
        db,
        // rp, TODO
        measurement,
        tagKey,
      },
      links,
    }

    const newRows = rows.map(r => (r.id === template.id ? templateVariable : r))

    this.setState({rows: newRows, isEdited: true})
  }

  onSaveTemplatesSuccess() {
    const {rows} = this.state

    const newRows = rows.map(row => ({...row, isNew: false}))

    this.setState({rows: newRows, isEdited: false})
  }

  onDeleteTemplateVariable(templateID) {
    const {rows} = this.state

    const newRows = rows.filter(({id}) => id !== templateID)

    this.setState({rows: newRows, isEdited: true})
  }

  tempVarAlreadyExists(testTempVar, testID) {
    const {rows: tempVars} = this.state
    return tempVars.some(
      ({tempVar, id}) => tempVar === testTempVar && id !== testID
    )
  }

  render() {
    const {rows, isEdited} = this.state
    return (
      <TemplateVariableManager
        {...this.props}
        onRunQuerySuccess={this.onRunQuerySuccess}
        onSaveTemplatesSuccess={this.onSaveTemplatesSuccess}
        onAddVariable={this.onAddVariable}
        templates={rows}
        isEdited={isEdited}
        onDelete={this.onDeleteTemplateVariable}
        tempVarAlreadyExists={this.tempVarAlreadyExists}
      />
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

TemplateVariableManager.propTypes = {
  ...TemplateVariableManagerWrapper.propTypes,
  onRunQuerySuccess: func.isRequired,
  onSaveTemplatesSuccess: func.isRequired,
  onAddVariable: func.isRequired,
  isEdited: bool.isRequired,
  onDelete: func.isRequired,
}

TemplateVariableManagerWrapper.propTypes = {
  onClose: func.isRequired,
  onEditTemplateVariables: func.isRequired,
  source: shape({
    links: shape({
      proxy: string,
    }),
  }).isRequired,
  templates: arrayOf(
    shape({
      type: string.isRequired,
      tempVar: string.isRequired,
      query: shape({
        db: string,
        influxql: string,
      }),
      values: arrayOf(
        shape({
          value: string.isRequired,
          type: string.isRequired,
          selected: bool.isRequired,
        })
      ).isRequired,
    })
  ),
  onRunQueryFailure: func.isRequired,
}

export default TemplateVariableManagerWrapper
