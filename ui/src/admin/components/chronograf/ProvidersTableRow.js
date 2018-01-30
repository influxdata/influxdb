import React, {Component, PropTypes} from 'react'

import ConfirmButtons from 'shared/components/ConfirmButtons'
import Dropdown from 'shared/components/Dropdown'
import InputClickToEdit from 'shared/components/InputClickToEdit'

import {DEFAULT_PROVIDER_MAP_ID} from 'src/admin/constants/dummyProviderMaps'

class ProvidersTableRow extends Component {
  constructor(props) {
    super(props)

    this.state = {
      scheme: this.props.mapping.scheme,
      provider: this.props.mapping.provider,
      providerOrganization: this.props.mapping.providerOrganization,
      organizationId: this.props.mapping.organizationId,
      isDeleting: false,
    }
  }

  handleDeleteClick = () => {
    this.setState({isDeleting: true})
  }

  handleDismissDeleteConfirmation = () => {
    this.setState({isDeleting: false})
  }

  handleDeleteMap = mapping => {
    const {onDelete} = this.props
    this.setState({isDeleting: false})
    onDelete(mapping)
  }

  handleChangeScheme = scheme => {
    this.setState({scheme})
    this.handleUpdateMapping()
  }

  handleChangeProvider = provider => {
    this.setState({provider})
    this.handleUpdateMapping()
  }

  handleChangeProviderOrg = providerOrganization => {
    this.setState({providerOrganization})
    this.handleUpdateMapping()
  }

  handleChooseOrganization = org => {
    this.setState({organizationId: org.id})
    this.handleUpdateMapping()
  }

  handleUpdateMapping = () => {
    const {onUpdate, mapping: {id}} = this.props
    const {scheme, provider, providerOrganization, organizationId} = this.state

    const updatedMap = {
      id,
      scheme,
      provider,
      providerOrganization,
      organizationId,
    }
    onUpdate(updatedMap)
  }

  render() {
    const {
      scheme,
      provider,
      providerOrganization,
      organizationId,
      isDeleting,
    } = this.state
    const {organizations, mapping} = this.props

    const selectedOrg = organizations.find(o => o.id === organizationId)

    const dropdownItems = organizations.map(role => ({
      ...role,
      text: role.name,
    }))

    const organizationIdClassName = isDeleting
      ? 'fancytable--td provider--redirect deleting'
      : 'fancytable--td provider--redirect'

    const isDefaultMapping = DEFAULT_PROVIDER_MAP_ID === mapping.id

    return (
      <div className="fancytable--row">
        <div className="fancytable--td provider--id">
          {mapping.id}
        </div>
        <InputClickToEdit
          value={scheme}
          wrapperClass="fancytable--td provider--scheme"
          onUpdate={this.handleChangeScheme}
          disabled={isDefaultMapping}
        />
        <InputClickToEdit
          value={provider}
          wrapperClass="fancytable--td provider--provider"
          onUpdate={this.handleChangeProvider}
          disabled={isDefaultMapping}
        />
        <InputClickToEdit
          value={providerOrganization}
          wrapperClass="fancytable--td provider--providerorg"
          onUpdate={this.handleChangeProviderOrg}
          disabled={isDefaultMapping}
        />
        <div className="fancytable--td provider--arrow">
          <span />
        </div>
        <div className={organizationIdClassName}>
          <Dropdown
            items={dropdownItems}
            onChoose={this.handleChooseOrganization}
            selected={selectedOrg.name}
            className="dropdown-stretch"
          />
        </div>
        {isDeleting
          ? <ConfirmButtons
              item={mapping}
              onCancel={this.handleDismissDeleteConfirmation}
              onConfirm={this.handleDeleteMap}
              onClickOutside={this.handleDismissDeleteConfirmation}
            />
          : <button
              className="btn btn-sm btn-default btn-square"
              onClick={this.handleDeleteClick}
            >
              <span className="icon trash" />
            </button>}
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

ProvidersTableRow.propTypes = {
  mapping: shape({
    id: string,
    scheme: string,
    provider: string,
    providerOrganization: string,
    organizationId: string,
  }),
  organizations: arrayOf(
    shape({
      id: string.isRequired,
      name: string.isRequired,
    })
  ),
  onDelete: func.isRequired,
  onUpdate: func.isRequired,
}

export default ProvidersTableRow
