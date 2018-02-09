import React, {Component, PropTypes} from 'react'

import ConfirmButtons from 'shared/components/ConfirmButtons'
import Dropdown from 'shared/components/Dropdown'
import InputClickToEdit from 'shared/components/InputClickToEdit'

import {DEFAULT_MAPPING_ID} from 'src/admin/constants/chronografAdmin'

class ProvidersTableRow extends Component {
  constructor(props) {
    super(props)

    this.state = {
      ...this.props.mapping,
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

  handleUpdateMapping = changes => {
    const {onUpdate, mapping} = this.props
    const newState = {...mapping, ...changes}
    this.setState(newState)
    onUpdate(mapping, newState)
  }

  handleChangeProvider = provider => this.handleUpdateMapping({provider})

  handleChangeProviderOrg = providerOrganization =>
    this.handleUpdateMapping({providerOrganization})

  handleChooseOrganization = ({id: organizationId}) =>
    this.handleUpdateMapping({organizationId})

  handleChooseScheme = ({text: scheme}) => this.handleUpdateMapping({scheme})

  render() {
    const {
      scheme,
      provider,
      providerOrganization,
      organizationId,
      isDeleting,
    } = this.state
    const {organizations, mapping, schemes, rowIndex} = this.props

    const selectedOrg = organizations.find(o => o.id === organizationId)
    const orgDropdownItems = organizations.map(role => ({
      ...role,
      text: role.name,
    }))

    const organizationIdClassName = isDeleting
      ? 'fancytable--td provider--redirect deleting'
      : 'fancytable--td provider--redirect'

    const isDefaultMapping = DEFAULT_MAPPING_ID === mapping.id
    return (
      <div className="fancytable--row">
        <Dropdown
          items={schemes}
          onChoose={this.handleChooseScheme}
          selected={scheme}
          className="fancytable--td provider--scheme"
          disabled={isDefaultMapping}
        />
        <InputClickToEdit
          value={provider}
          wrapperClass="fancytable--td provider--provider"
          onUpdate={this.handleChangeProvider}
          disabled={isDefaultMapping}
          tabIndex={rowIndex}
        />
        <InputClickToEdit
          value={providerOrganization}
          wrapperClass="fancytable--td provider--providerorg"
          onUpdate={this.handleChangeProviderOrg}
          disabled={isDefaultMapping}
          tabIndex={rowIndex}
        />
        <div className="fancytable--td provider--arrow">
          <span />
        </div>
        <div className={organizationIdClassName}>
          <Dropdown
            items={orgDropdownItems}
            onChoose={this.handleChooseOrganization}
            selected={selectedOrg.name}
            className="dropdown-stretch"
            disabled={isDefaultMapping}
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

const {arrayOf, func, number, shape, string} = PropTypes

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
  schemes: arrayOf(
    shape({
      text: string.isRequired,
    })
  ),
  rowIndex: number,
  onDelete: func.isRequired,
  onUpdate: func.isRequired,
}

export default ProvidersTableRow
