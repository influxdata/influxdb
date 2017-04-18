import React, {PropTypes, Component} from 'react'
import Dropdown from 'shared/components/Dropdown'
import TemplateValues from 'src/dashboards/components/TemplateValues'

import {TEMPLATE_VARIBALE_TYPES} from 'src/dashboards/constants'

const TemplateVariableRow = ({
  template: {label, code, values},
  selectedType,
  handleSelectType,
}) => (
  <tr>
    <td>{label}</td>
    <td>{code}</td>
    <td>
      <Dropdown
        items={TEMPLATE_VARIBALE_TYPES}
        onChoose={handleSelectType}
        selected={selectedType}
        className={'template-variable--dropdown'}
      />
    </td>
    <td>
      <TemplateValues values={values} selectedType={selectedType} />
    </td>
    <td>
      <button className="btn btn-sm btn-danger" type="button">Delete</button>
    </td>
  </tr>
)

class RowWrapper extends Component {
  constructor(props) {
    super(props)
    this.state = {
      selectedType: this.props.template.type,
    }

    this.handleSelectType = ::this.handleSelectType
  }

  handleSelectType(item) {
    this.setState({selectedType: item.type})
  }

  render() {
    return (
      <TemplateVariableRow
        {...this.props}
        selectedType={this.state.selectedType}
        handleSelectType={this.handleSelectType}
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
  handleSelectType: func.isRequired,
}

export default RowWrapper
