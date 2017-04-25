import React, {Component, PropTypes} from 'react'
import OnClickOutside from 'react-onclickoutside'
import TemplateVariableTable
  from 'src/dashboards/components/TemplateVariableTable'

import {TEMPLATE_VARIABLE_TYPES} from 'src/dashboards/constants'

const TemplateVariableManager = ({
  onClose,
  onEditTemplateVariables,
  source,
  templates,
  onRunQuerySuccess,
  onRunQueryFailure,
}) => (
  <div className="template-variable-manager">
    <div className="template-variable-manager--header">
      <div className="page-header__left">
        Template Variables
      </div>
      <div className="page-header__right">
        <button className="btn btn-primary btn-sm">Add Variable</button>
        <button
          className="btn btn-primary btn-sm"
          onClick={onEditTemplateVariables(templates)}
        >
          Save Template
        </button>
        <span
          className="icon remove"
          onClick={onClose}
          style={{cursor: 'pointer'}}
        />
      </div>
    </div>
    <div className="template-variable-manager--body">
      <TemplateVariableTable
        source={source}
        templates={templates}
        onRunQuerySuccess={onRunQuerySuccess}
        onRunQueryFailure={onRunQueryFailure}
      />
    </div>
  </div>
)

class TemplateVariableManagerWrapper extends Component {
  constructor(props) {
    super(props)

    this.state = {
      rows: this.props.templates,
    }

    this.onRunQuerySuccess = ::this.onRunQuerySuccess
  }

  onRunQuerySuccess(template, queryConfig, parsedData, {tempVar, label}) {
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
    const currentRow = rows.find(row => row.tempVar === tempVar)

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
      label,
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

    this.setState({rows: newRows})
  }

  render() {
    return (
      <TemplateVariableManager
        {...this.props}
        onRunQuerySuccess={this.onRunQuerySuccess}
        templates={this.state.rows}
      />
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

TemplateVariableManager.propTypes = {
  ...TemplateVariableManagerWrapper.propTypes,
  onRunQuerySuccess: func.isRequired,
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
      label: string.isRequired,
      tempVar: string.isRequired,
      query: shape({
        db: string.isRequired,
        influxql: string.isRequired,
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

export default OnClickOutside(TemplateVariableManagerWrapper)
