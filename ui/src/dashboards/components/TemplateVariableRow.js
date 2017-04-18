import React, {PropTypes, Component} from 'react'
import Dropdown from 'shared/components/Dropdown'
import TemplateQueries from 'src/dashboards/components/TemplateQueries'

import {TEMPLATE_VARIBALE_TYPES} from 'src/dashboards/constants'

const TemplateVariableRow = ({
  template: {label, code, values},
  selectedType,
  selectedDatabase,
  selectedMeasurement,
  onSelectType,
  onSelectDatabase,
  onSelectMeasurement,
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
      <TemplateQueries
        onSelectDatabase={onSelectDatabase}
        selectedType={selectedType}
        selectedDatabase={selectedDatabase}
        onSelectMeasurement={onSelectMeasurement}
        selectedMeasurement={selectedMeasurement}
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
    }

    this.handleSelectType = ::this.handleSelectType
    this.handleSelectDatabase = ::this.handleSelectDatabase
    this.handleSelectMeasurement = ::this.handleSelectMeasurement
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

  render() {
    const {selectedType, selectedDatabase, selectedMeasurement} = this.state
    return (
      <TemplateVariableRow
        {...this.props}
        selectedType={selectedType}
        selectedDatabase={selectedDatabase}
        selectedMeasurement={selectedMeasurement}
        onSelectType={this.handleSelectType}
        onSelectDatabase={this.handleSelectDatabase}
        onSelectMeasurement={this.handleSelectMeasurement}
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
      db: string.isRequired,
      text: string.isRequired,
    }),
    values: arrayOf(string.isRequired),
  }),
}

TemplateVariableRow.propTypes = {
  ...RowWrapper.propTypes,
  selectedType: string.isRequired,
  selectedDatabase: string,
  onSelectType: func.isRequired,
  onSelectDatabase: func.isRequired,
}

export default RowWrapper
