import React, {Component, PropTypes} from 'react'

import Authorized, {SUPERADMIN_ROLE} from 'src/auth/Authorized'

import Dropdown from 'shared/components/Dropdown'
import SlideToggle from 'shared/components/SlideToggle'

import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'
import {USER_ROLES} from 'src/admin/constants/chronografAdmin'
import {MEMBER_ROLE} from 'src/auth/Authorized'

class UsersTableRowNew extends Component {
  constructor(props) {
    super(props)

    this.state = {
      name: '',
      provider: '',
      scheme: 'oauth2',
      role: MEMBER_ROLE,
      superAdmin: false,
    }
  }

  handleInputChange = fieldName => e => {
    this.setState({[fieldName]: e.target.value.trim()})
  }

  handleConfirmCreateUser = () => {
    const {onBlur, onCreateUser, organization} = this.props
    const {name, provider, scheme, role, superAdmin} = this.state

    const newUser = {
      name,
      provider,
      scheme,
      superAdmin,
      roles: [
        {
          name: role,
          organization: organization.id,
        },
      ],
    }

    onCreateUser(newUser)
    onBlur()
  }

  handleInputFocus = e => {
    e.target.select()
  }

  handleSelectRole = newRole => {
    this.setState({role: newRole.text})
  }

  handleSelectSuperAdmin = superAdmin => {
    this.setState({superAdmin})
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
          'Users must have a name and auth provider.'
        )
      }
      this.handleConfirmCreateUser()
    }
  }

  render() {
    const {
      colRole,
      colProvider,
      colScheme,
      colSuperAdmin,
      colActions,
    } = USERS_TABLE
    const {onBlur} = this.props
    const {name, provider, scheme, role, superAdmin} = this.state

    const dropdownRolesItems = USER_ROLES.map(r => ({...r, text: r.name}))
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
        <td style={{width: colRole}}>
          <Dropdown
            items={dropdownRolesItems}
            selected={role}
            onChoose={this.handleSelectRole}
            buttonColor="btn-primary"
            buttonSize="btn-xs"
            className="dropdown-stretch"
          />
        </td>
        <Authorized requiredRole={SUPERADMIN_ROLE}>
          <td style={{width: colSuperAdmin}} className="text-center">
            <SlideToggle
              active={superAdmin}
              size="xs"
              onToggle={this.handleSelectSuperAdmin}
            />
          </td>
        </Authorized>
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

const {func, shape, string} = PropTypes

UsersTableRowNew.propTypes = {
  organization: shape({
    id: string.isRequired,
    name: string.isRequired,
  }),
  onBlur: func.isRequired,
  onCreateUser: func.isRequired,
  notify: func.isRequired,
}

export default UsersTableRowNew
