import React, {Component, PropTypes} from 'react'

import Authorized, {SUPERADMIN_ROLE} from 'src/auth/Authorized'

import Dropdown from 'shared/components/Dropdown'
import SlideToggle from 'shared/components/SlideToggle'

import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'
import {DEFAULT_ORG_ID} from 'src/admin/constants/dummyUsers'

class NewUserTableRow extends Component {
  constructor(props) {
    super(props)

    this.state = {
      name: '',
      provider: '',
      scheme: 'oauth2',
      role: null,
      superAdmin: false,
      organization: this.props.currentOrganization,
    }
  }

  handleInputChange = fieldName => e => {
    this.setState({[fieldName]: e.target.value.trim()})
  }

  handleClickCreateUser = () => {
    const {onCancelCreateUser, onCreateUser} = this.props
    const {name, provider, scheme, role, superAdmin, organization} = this.state

    const newUser = {
      name,
      provider,
      scheme,
      superAdmin: superAdmin.value,
      roles: [
        {
          name: role,
          organization: organization.id,
        },
      ],
    }

    onCreateUser(newUser)
    onCancelCreateUser()
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

  handleSelectOrganization = newUserOrganization => {
    this.setState({organization: newUserOrganization})
  }

  render() {
    const {
      colOrg,
      colRole,
      colProvider,
      colScheme,
      colSuperAdmin,
      colActions,
    } = USERS_TABLE
    const {
      onCancelCreateUser,
      roles,
      organizations,
      currentOrganization,
    } = this.props
    const {name, provider, scheme, role, superAdmin, organization} = this.state

    const preventCreate = !name || !provider || !role || !organization

    const isDefaultOrg = currentOrganization.id === DEFAULT_ORG_ID

    const organizationsMinusDefault = organizations.filter(org => {
      if (org.id !== DEFAULT_ORG_ID) {
        return org
      }
    })

    return (
      <tr className="chronograf-admin-table--new-user">
        <td className="chronograf-admin-table--check-col" />
        <td>
          <input
            className="form-control input-xs"
            type="text"
            placeholder="OAuth Username..."
            autoFocus={true}
            value={name}
            onChange={this.handleInputChange('name')}
          />
        </td>
        <td style={{width: colOrg}}>
          {isDefaultOrg
            ? <Dropdown
                items={organizationsMinusDefault.map(org => ({
                  ...org,
                  text: org.name,
                }))}
                selected={
                  organization.id === DEFAULT_ORG_ID
                    ? 'Choose one'
                    : organization.name
                }
                onChoose={this.handleSelectOrganization}
                buttonColor="btn-primary"
                buttonSize="btn-xs"
                className="dropdown-stretch"
              />
            : <span className="chronograf-user--org">
                {currentOrganization.name}
              </span>}
        </td>
        <td style={{width: colRole}}>
          <Dropdown
            items={roles.map(r => ({...r, text: r.name}))}
            selected={role || 'Assign'}
            onChoose={this.handleSelectRole}
            buttonColor="btn-primary"
            buttonSize="btn-xs"
            className="dropdown-80"
            disabled={organization.id === DEFAULT_ORG_ID}
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
          <button
            className="btn btn-xs btn-square btn-info"
            onClick={onCancelCreateUser}
          >
            <span className="icon remove" />
          </button>
          <button
            className="btn btn-xs btn-square btn-success"
            disabled={preventCreate}
            onClick={this.handleClickCreateUser}
          >
            <span className="icon checkmark" />
          </button>
        </td>
      </tr>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

NewUserTableRow.propTypes = {
  currentOrganization: shape({
    id: string.isRequired,
    name: string.isRequired,
  }),
  onCancelCreateUser: func.isRequired,
  onCreateUser: func.isRequired,
  roles: arrayOf(shape()).isRequired,
  organizations: arrayOf(
    shape({
      id: string.isRequired,
      name: string.isRequired,
    })
  ).isRequired,
}

export default NewUserTableRow
