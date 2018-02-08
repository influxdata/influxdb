import React, {Component, PropTypes} from 'react'

import Dropdown from 'shared/components/Dropdown'

import {ALL_USERS_TABLE} from 'src/admin/constants/chronografTableSizing'
const {
  colOrganizations,
  colProvider,
  colScheme,
  colSuperAdmin,
  colActions,
} = ALL_USERS_TABLE

const nullOrganization = {id: undefined, name: 'None'}
const nullRole = {name: '*', organization: undefined}

class AllUsersTableRowNew extends Component {
  constructor(props) {
    super(props)

    this.state = {
      name: '',
      provider: '',
      scheme: 'oauth2',
      role: {
        ...nullRole,
      },
    }
  }

  handleInputChange = fieldName => e => {
    this.setState({[fieldName]: e.target.value.trim()})
  }

  handleConfirmCreateUser = () => {
    const {onBlur, onCreateUser} = this.props
    const {name, provider, scheme, role, superAdmin} = this.state
    const newUser = {
      name,
      provider,
      scheme,
      superAdmin,
      // since you can only choose one organization, there is only one role in a new row
      // if no organization is selected ie the "None" organization,
      // then set roles to an empty array
      roles: role.organization === undefined ? [] : [role],
    }
    onCreateUser(newUser)
    onBlur()
  }

  handleInputFocus = e => {
    e.target.select()
  }

  handleSelectOrganization = newOrganization => {
    // if "None" was selected for organization, create a "null role" from the predefined null role
    // else create a new role with the organization as the newOrganization's id
    const newRole =
      newOrganization.id === undefined
        ? {
            ...nullRole,
          }
        : {
            organization: newOrganization.id,
            name: '*', // '*' causes the server to determine the current defaultRole of the selected organization
          }
    this.setState({role: newRole})
  }

  handleKeyDown = e => {
    const {name, provider} = this.state
    const preventCreate = !name || !provider

    if (e.key === 'Escape') {
      this.props.onBlur()
    }

    if (e.key === 'Enter') {
      if (preventCreate) {
        return this.props.notify(
          'warning',
          'User must have a name and provider'
        )
      }
      this.handleConfirmCreateUser()
    }
  }

  render() {
    const {organizations, onBlur} = this.props
    const {name, provider, scheme, role} = this.state

    const dropdownOrganizationsItems = [
      {...nullOrganization},
      ...organizations,
    ].map(o => ({
      ...o,
      text: o.name,
    }))
    const selectedRole = dropdownOrganizationsItems.find(
      o => role.organization === o.id
    )

    const preventCreate = !name || !provider

    return (
      <tr className="chronograf-admin-table--new-user">
        <td>
          <input
            className="form-control input-xs"
            type="text"
            placeholder="OAuth Username..."
            autoFocus={true}
            value={name}
            onChange={this.handleInputChange('name')}
            onKeyDown={this.handleKeyDown}
          />
        </td>
        <td style={{width: colOrganizations}}>
          <Dropdown
            items={dropdownOrganizationsItems}
            selected={selectedRole.text}
            onChoose={this.handleSelectOrganization}
            buttonColor="btn-primary"
            buttonSize="btn-xs"
            className="dropdown-stretch"
          />
        </td>
        <td style={{width: colProvider}}>
          <input
            className="form-control input-xs"
            type="text"
            placeholder="OAuth Provider..."
            value={provider}
            onChange={this.handleInputChange('provider')}
            onKeyDown={this.handleKeyDown}
          />
        </td>
        <td style={{width: colScheme}}>
          <input
            className="form-control input-xs disabled"
            type="text"
            disabled={true}
            placeholder="OAuth Scheme..."
            value={scheme}
          />
        </td>
        <td style={{width: colSuperAdmin}} className="text-center">
          &mdash;
        </td>
        <td className="text-right" style={{width: colActions}}>
          <button className="btn btn-xs btn-square btn-info" onClick={onBlur}>
            <span className="icon remove" />
          </button>
          <button
            className="btn btn-xs btn-square btn-success"
            disabled={preventCreate}
            onClick={this.handleConfirmCreateUser}
          >
            <span className="icon checkmark" />
          </button>
        </td>
      </tr>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

AllUsersTableRowNew.propTypes = {
  organizations: arrayOf(
    shape({
      id: string.isRequired,
      name: string.isRequired,
    })
  ),
  onBlur: func.isRequired,
  onCreateUser: func.isRequired,
  notify: func.isRequired,
}

export default AllUsersTableRowNew
