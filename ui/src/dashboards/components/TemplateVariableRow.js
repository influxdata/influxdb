import React, {PropTypes, Component} from 'react'
import OnClickOutside from 'react-onclickoutside'
import Dropdown from 'shared/components/Dropdown'
import TemplateQueryBuilder
  from 'src/dashboards/components/TemplateQueryBuilder'

import {TEMPLATE_VARIBALE_TYPES} from 'src/dashboards/constants'

const TemplateVariableRow = ({
  template: {label, code, values},
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
  autoFocusTarget,
  onSubmit,
}) => (
  <form className="tr" onSubmit={onSubmit}>
    <TableInput
      name="label"
      defaultValue={label}
      isEditing={isEditing}
      onStartEdit={onStartEdit}
      autoFocusTarget={autoFocusTarget}
    />
    <TableInput
      name="code"
      defaultValue={code}
      isEditing={isEditing}
      onStartEdit={onStartEdit}
      autoFocusTarget={autoFocusTarget}
    />
    <div className="td">
      <Dropdown
        items={TEMPLATE_VARIBALE_TYPES}
        onChoose={onSelectType}
        selected={selectedType}
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
      />
    </div>
    <div className="td">
      {values.join(', ')}
    </div>
    <div className="td" style={{display: 'flex'}}>
      {isEditing
        ? <div>
            <button className="btn btn-sm btn-success" type="submit">
              Submit
            </button>
            <button className="btn btn-sm btn-primary" type="button">
              Cancel
            </button>
          </div>
        : <button className="btn btn-sm btn-danger" type="button">
            Delete
          </button>}
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

    this.handleSelectType = ::this.handleSelectType
    this.handleSelectDatabase = ::this.handleSelectDatabase
    this.handleSelectMeasurement = ::this.handleSelectMeasurement
    this.handleSelectTagKey = ::this.handleSelectTagKey
    this.handleStartEdit = ::this.handleStartEdit
    this.handleEndEdit = ::this.handleEndEdit
  }

  handleSubmit(e) {
    e.preventDefault()
    // const code = e.target.code.value
    // const label = e.target.label.value

    // updateTempVarsAsync({code, label})
  }

  handleClickOutside() {
    this.setState({isEditing: false})
  }

  handleEndEdit() {
    this.setState({isEditing: false})
  }

  handleStartEdit(name) {
    this.setState({isEditing: true, autoFocusTarget: name})
  }

  handleSelectType(item) {
    this.setState({selectedType: item.type})
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
        autoFocusTarget={autoFocusTarget}
        onSubmit={this.handleSubmit}
      />
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

RowWrapper.propTypes = {
  template: shape({
    type: string.isRequired,
    label: string.isRequired,
    code: string.isRequired,
    query: shape({
      db: string,
      text: string.isRequired,
      measurement: string,
      tagKey: string,
    }),
    values: arrayOf(string.isRequired),
  }),
}

TemplateVariableRow.propTypes = {
  ...RowWrapper.propTypes,
  selectedType: string.isRequired,
  selectedDatabase: string,
  selectedTagKey: string,
  onSelectType: func.isRequired,
  onSelectDatabase: func.isRequired,
  onSelectTagKey: func.isRequired,
}

TableInput.propTypes = {
  defaultValue: string,
  isEditing: bool.isRequired,
  onStartEdit: func.isRequired,
  name: string.isRequired,
  autoFocusTarget: string,
}

export default OnClickOutside(RowWrapper)
