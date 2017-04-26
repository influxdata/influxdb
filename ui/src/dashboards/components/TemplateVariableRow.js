import React, {PropTypes, Component} from 'react'
import OnClickOutside from 'react-onclickoutside'

import Dropdown from 'shared/components/Dropdown'
import DeleteConfirmButtons from 'shared/components/DeleteConfirmButtons'
import TemplateQueryBuilder
  from 'src/dashboards/components/TemplateQueryBuilder'

import {
  runTemplateVariableQuery as runTemplateVariableQueryAJAX,
} from 'src/dashboards/apis'

import parsers from 'shared/parsing'

import {TEMPLATE_TYPES} from 'src/dashboards/constants'
import q
  from 'src/dashboards/utils/onlyTheBigliestBigLeagueTemplateVariableQueryGenerator'

const RowValues = ({
  selectedType,
  values = [],
  isEditing,
  onStartEdit,
  autoFocusTarget,
}) => {
  const _values = values.map(({value}) => value).join(', ')

  if (selectedType === 'csv') {
    return (
      <TableInput
        name="values"
        defaultValue={_values}
        isEditing={isEditing}
        onStartEdit={onStartEdit}
        autoFocusTarget={autoFocusTarget}
      />
    )
  }
  return values.length
    ? <span>{_values}</span>
    : <span>(No values to display)</span>
}

const TemplateVariableRow = ({
  template: {id, label, tempVar, values},
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
  onDelete,
}) => (
  <form
    className="tr"
    onSubmit={onSubmit({
      selectedType,
      selectedDatabase,
      selectedMeasurement,
      selectedTagKey,
    })}
  >
    <TableInput
      name="label"
      defaultValue={label}
      isEditing={isEditing}
      onStartEdit={onStartEdit}
      autoFocusTarget={autoFocusTarget}
    />
    <TableInput
      name="tempVar"
      defaultValue={tempVar}
      isEditing={isEditing}
      onStartEdit={onStartEdit}
      autoFocusTarget={autoFocusTarget}
    />
    <div className="td">
      <Dropdown
        items={TEMPLATE_TYPES}
        onChoose={onSelectType}
        onClick={() => onStartEdit(null)}
        selected={TEMPLATE_TYPES.find(t => t.type === selectedType).text}
        className={'template-variable--dropdown'}
      />
    </div>
    <div className="td">
      <TemplateQueryBuilder
        onSelectDatabase={onSelectDatabase}
        selectedType={selectedType}
        selectedDatabase={selectedDatabase}
        onSelectMeasurement={onSelectMeasurement}
        selectedMeasurement={selectedMeasurement}
        selectedTagKey={selectedTagKey}
        onSelectTagKey={onSelectTagKey}
        onStartEdit={onStartEdit}
      />
    </div>
    <div className="td">
      <RowValues
        selectedType={selectedType}
        values={values}
        isEditing={isEditing}
        onStartEdit={onStartEdit}
        autoFocusTarget={autoFocusTarget}
      />
    </div>
    <div className="td" style={{display: 'flex'}}>
      {isEditing
        ? <div>
            <button className="btn btn-sm btn-success" type="submit">
              Run Query
            </button>
            <button
              className="btn btn-sm btn-primary"
              type="button"
              onClick={onCancelEdit}
            >
              Cancel
            </button>
          </div>
        : <DeleteConfirmButtons onDelete={() => onDelete(id)} />}
    </div>
  </form>
)

const TableInput = ({
  name,
  defaultValue,
  isEditing,
  onStartEdit,
  autoFocusTarget,
}) => {
  return isEditing
    ? <div name={name} className="td">
        <input
          required={true}
          name={name}
          autoFocus={name === autoFocusTarget}
          className="input"
          type="text"
          defaultValue={defaultValue}
        />
      </div>
    : <div className="td" onClick={() => onStartEdit(name)}>{defaultValue}</div>
}

class RowWrapper extends Component {
  constructor(props) {
    super(props)
    const {template: {query, type}} = this.props

    this.state = {
      isEditing: false,
      selectedType: type,
      selectedDatabase: query && query.db,
      selectedMeasurement: query && query.measurement,
      selectedTagKey: query && query.tagKey,
      autoFocusTarget: null,
    }

    this.handleRunQueryRequested = ::this.handleRunQueryRequested
    this.handleSelectType = ::this.handleSelectType
    this.handleSelectDatabase = ::this.handleSelectDatabase
    this.handleSelectMeasurement = ::this.handleSelectMeasurement
    this.handleSelectTagKey = ::this.handleSelectTagKey
    this.handleStartEdit = ::this.handleStartEdit
    this.handleCancelEdit = ::this.handleCancelEdit
    this.runTemplateVariableQuery = ::this.runTemplateVariableQuery
  }

  handleRunQueryRequested({
    selectedDatabase: database,
    selectedMeasurement: measurement,
    selectedTagKey: tagKey,
    selectedType: type,
  }) {
    return async e => {
      e.preventDefault()

      this.setState({isEditing: false})

      const label = e.target.label.value
      const tempVar = e.target.tempVar.value

      const {
        source,
        template,
        onRunQuerySuccess,
        onRunQueryFailure,
      } = this.props

      const {query, tempVars} = q({
        type,
        label,
        tempVar,
        query: {
          database,
          // rp, TODO
          measurement,
          tagKey,
        },
      })

      const queryConfig = {
        query,
        database,
        // rp: TODO
        tempVars,
        type,
        measurement,
        tagKey,
      }

      try {
        const parsedData = await this.runTemplateVariableQuery(
          source,
          queryConfig
        )
        onRunQuerySuccess(template, queryConfig, parsedData, {tempVar, label})
      } catch (error) {
        onRunQueryFailure(error)
      }
    }
  }

  handleClickOutside() {
    this.setState({isEditing: false})
  }

  handleStartEdit(name) {
    this.setState({isEditing: true, autoFocusTarget: name})
  }

  handleCancelEdit() {
    const {template: {type, query: {db, measurement, tagKey}}} = this.props
    this.setState({
      selectedType: type,
      selectedDatabase: db,
      selectedMeasurement: measurement,
      selectedKey: tagKey,
      isEditing: false,
    })
  }

  handleSelectType(item) {
    this.setState({
      selectedType: item.type,
      selectedDatabase: null,
      selectedMeasurement: null,
      selectedKey: null,
    })
  }

  handleSelectDatabase(item) {
    this.setState({selectedDatabase: item.text})
  }

  handleSelectMeasurement(item) {
    this.setState({selectedMeasurement: item.text})
  }

  handleSelectTagKey(item) {
    this.setState({selectedTagKey: item.text})
  }

  async runTemplateVariableQuery(
    source,
    {query, database, rp, tempVars, type, measurement, tagKey}
  ) {
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
        onSubmit={this.handleRunQueryRequested}
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
    label: string.isRequired,
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
}

TableInput.propTypes = {
  defaultValue: string,
  isEditing: bool.isRequired,
  onStartEdit: func.isRequired,
  name: string.isRequired,
  autoFocusTarget: string,
}

RowValues.propTypes = {
  selectedType: string.isRequired,
  values: arrayOf(shape()),
  isEditing: bool.isRequired,
  onStartEdit: func.isRequired,
  autoFocusTarget: string,
}

export default OnClickOutside(RowWrapper)
