import React, {Component, PropTypes} from 'react'

import ConfirmButtons from 'shared/components/ConfirmButtons'
import Dropdown from 'shared/components/Dropdown'
import InputClickToEdit from 'shared/components/InputClickToEdit'

class ProvidersTableRowNew extends Component {
  constructor(props) {
    super(props)

    this.state = {
      scheme: '*',
      provider: null,
      providerOrganization: null,
      organizationId: 'default',
    }
  }

  handleChooseScheme = scheme => {
    this.setState({scheme: scheme.text})
  }

  handleChangeProvider = provider => {
    this.setState({provider})
  }

  handleChangeProviderOrg = providerOrganization => {
    this.setState({providerOrganization})
  }

  handleChooseOrganization = org => {
    this.setState({organizationId: org.id})
  }

  handleSaveNewMapping = () => {
    const {scheme, provider, providerOrganization, organizationId} = this.state
    const {onCreate} = this.props
    // id is calculated in providers table
    onCreate({id: '', scheme, provider, providerOrganization, organizationId})
  }

  render() {
    const {scheme, provider, providerOrganization, organizationId} = this.state

    const {organizations, onCancel, schemes} = this.props

    const selectedOrg = organizations.find(o => o.id === organizationId)

    const dropdownItems = organizations.map(role => ({
      ...role,
      text: role.name,
    }))

    return (
      <div className="fancytable--row">
        <div className="fancytable--td provider--id">--</div>

        <Dropdown
          items={schemes}
          onChoose={this.handleChooseScheme}
          selected={scheme}
          className={'fancytable--td provider--scheme'}
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
            selected={selectedOrg.name}
            className="dropdown-stretch"
          />
        </div>
        <ConfirmButtons
          onCancel={onCancel}
          onConfirm={this.handleSaveNewMapping}
        />
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
  schemes: arrayOf(
    shape({
      text: string.isRequired,
    })
  ),
  onCreate: func.isRequired,
  onCancel: func.isRequired,
}

export default ProvidersTableRowNew
