import React, {PropTypes, Component} from 'react'
import Dropdown from 'shared/components/Dropdown'
import TemplateQueryBuilder
  from 'src/dashboards/components/TemplateQueryBuilder'

import {TEMPLATE_VARIBALE_TYPES} from 'src/dashboards/constants'

const TemplateVariableRow = ({
  template: {label, code, values},
  selectedType,
  selectedDatabase,
  selectedMeasurement,
  onSelectType,
  onSelectDatabase,
  onSelectMeasurement,
  selectedTagKey,
  onSelectTagKey,
}) => (
  <tr>
    <td>{label}</td>
    <td>{code}</td>
    <td>
      <Dropdown
        items={TEMPLATE_VARIBALE_TYPES}
        onChoose={onSelectType}
        selected={selectedType}
        className={'template-variable--dropdown'}
      />
    </td>
    <td>
      <TemplateQueryBuilder
        onSelectDatabase={onSelectDatabase}
        selectedType={selectedType}
        selectedDatabase={selectedDatabase}
        onSelectMeasurement={onSelectMeasurement}
        selectedMeasurement={selectedMeasurement}
        selectedTagKey={selectedTagKey}
        onSelectTagKey={onSelectTagKey}
      />
    </td>
    <td>
      {values.join(', ')}
    </td>
    <td>
      <button className="btn btn-sm btn-danger" type="button">Delete</button>
    </td>
  </tr>
)

class RowWrapper extends Component {
  constructor(props) {
    super(props)
    const {template: {query, type}} = this.props

    this.state = {
      selectedType: type,
      selectedDatabase: query && query.db,
      selectedMeasurement: query && query.measurement,
      selectedTagKey: query && query.tagKey,
    }

    this.handleSelectType = ::this.handleSelectType
    this.handleSelectDatabase = ::this.handleSelectDatabase
    this.handleSelectMeasurement = ::this.handleSelectMeasurement
    this.handleSelectTagKey = ::this.handleSelectTagKey
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
      selectedType,
      selectedDatabase,
      selectedMeasurement,
      selectedTagKey,
    } = this.state
    return (
      <TemplateVariableRow
        {...this.props}
        selectedType={selectedType}
        selectedDatabase={selectedDatabase}
        selectedMeasurement={selectedMeasurement}
        selectedTagKey={selectedTagKey}
        onSelectType={this.handleSelectType}
        onSelectDatabase={this.handleSelectDatabase}
        onSelectMeasurement={this.handleSelectMeasurement}
        onSelectTagKey={this.handleSelectTagKey}
      />
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

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

export default RowWrapper
