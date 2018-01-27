import React, {Component, PropTypes} from 'react'

import ConfirmButtons from 'shared/components/ConfirmButtons'
import Dropdown from 'shared/components/Dropdown'
import InputClickToEdit from 'shared/components/InputClickToEdit'

class ProvidersTableRowNew extends Component {
  constructor(props) {
    super(props)

    this.state = {
      scheme: null,
      provider: null,
      providerOrganization: null,
      redirectOrg: {name: '--'},
    }
  }

  handleChangeScheme = scheme => {
    this.setState({scheme})
  }

  handleChangeProvider = provider => {
    this.setState({provider})
  }

  handleChangeProviderOrg = providerOrganization => {
    this.setState({providerOrganization})
  }

  handleChooseOrganization = org => {
    this.setState({redirectOrg: org})
  }

  render() {
    const {scheme, provider, providerOrganization, redirectOrg} = this.state

    const {organizations, onCreate, onCancel} = this.props

    const dropdownItems = organizations.map(role => ({
      ...role,
      text: role.name,
    }))

    return (
      <div className="fancytable--row">
        <div className="fancytable--td provider--id">--</div>
        <InputClickToEdit
          value={scheme}
          wrapperClass="fancytable--td provider--scheme"
          onUpdate={this.handleChangeScheme}
        />
        <InputClickToEdit
          value={provider}
          wrapperClass="fancytable--td provider--provider"
          onUpdate={this.handleChangeProvider}
        />
        <InputClickToEdit
          value={providerOrganization}
          wrapperClass="fancytable--td provider--providerorg"
          onUpdate={this.handleChangeProviderOrg}
        />
        <div className="fancytable--td provider--arrow">
          <span />
        </div>
        <div className="fancytable--td provider--redirect deleting">
          <Dropdown
            items={dropdownItems}
            onChoose={this.handleChooseOrganization}
            selected={redirectOrg.name}
            className="dropdown-stretch"
          />
        </div>
        <ConfirmButtons onCancel={onCancel} onConfirm={onCreate} />
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

ProvidersTableRowNew.propTypes = {
  organizations: arrayOf(
    shape({
      id: string.isRequired,
      name: string.isRequired,
    })
  ).isRequired,
  onCreate: func.isRequired,
  onCancel: func.isRequired,
}

export default ProvidersTableRowNew
