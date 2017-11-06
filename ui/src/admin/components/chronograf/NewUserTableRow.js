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
      userName: '',
      userProvider: '',
      userScheme: 'oauth2',
      userRole: null,
      userSuperAdmin: false,
      userOrganization: this.props.currentOrganization,
    }
  }

  handleInputChange = fieldName => e => {
    this.setState({[fieldName]: e.target.value.trim()})
  }

  handleClickCreateUser = () => {
    const {onCancelCreateUser, onCreateUser} = this.props
    const {
      userName,
      userProvider,
      userScheme,
      userRole,
      userSuperAdmin,
      userOrganization,
    } = this.state

    const newUser = {
      name: userName,
      provider: userProvider,
      scheme: userScheme,
      superAdmin: userSuperAdmin.value,
      roles: [
        {
          name: userRole,
          organization: userOrganization.id,
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
    this.setState({userRole: newRole.text})
  }

  handleSelectSuperAdmin = userSuperAdmin => {
    this.setState({userSuperAdmin})
  }

  handleSelectOrganization = newUserOrganization => {
    this.setState({userOrganization: newUserOrganization})
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
      userRoles,
      organizations,
      currentOrganization,
    } = this.props
    const {
      userName,
      userProvider,
      userScheme,
      userRole,
      userSuperAdmin,
      userOrganization,
    } = this.state

    const allowCreate =
      !userName || !userProvider || !userRole || !userOrganization

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
            value={userName}
            onChange={this.handleInputChange('userName')}
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
                  userOrganization.id === DEFAULT_ORG_ID
                    ? 'Choose one'
                    : userOrganization.name
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
            items={userRoles.map(role => ({...role, text: role.name}))}
            selected={userRole || 'Assign'}
            onChoose={this.handleSelectRole}
            buttonColor="btn-primary"
            buttonSize="btn-xs"
            className="dropdown-80"
            disabled={userOrganization.id === DEFAULT_ORG_ID}
          />
        </td>
        <Authorized requiredRole={SUPERADMIN_ROLE}>
          <td style={{width: colSuperAdmin}} className="text-center">
            <SlideToggle
              active={userSuperAdmin}
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
            value={userProvider}
            onChange={this.handleInputChange('userProvider')}
          />
        </td>
        <td style={{width: colScheme}}>
          <input
            className="form-control input-xs disabled"
            type="text"
            disabled={true}
            placeholder="OAuth Scheme..."
            value={userScheme}
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
            disabled={allowCreate}
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
  userRoles: arrayOf(shape()).isRequired,
  organizations: arrayOf(
    shape({
      id: string.isRequired,
      name: string.isRequired,
    })
  ).isRequired,
}

export default NewUserTableRow
