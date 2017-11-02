import React, {Component, PropTypes} from 'react'

import Authorized, {SUPERADMIN_ROLE} from 'src/auth/Authorized'

import Dropdown from 'shared/components/Dropdown'

import {NO_ROLE} from 'src/admin/constants/dummyUsers'

const superAdminItems = [
  {value: true, text: 'True'},
  {value: false, text: 'False'},
]

class CreateUserOverlay extends Component {
  constructor(props) {
    super(props)

    this.state = {
      userName: '',
      userProvider: '',
      userScheme: 'OAuth2',
      userRole: NO_ROLE,
      userSuperAdmin: superAdminItems[1],
      userOrganization: null,
    }
  }

  handleInputChange = fieldName => e => {
    this.setState({[fieldName]: e.target.value.trim()})
  }

  handleClickCreateUser = () => {
    const {onDismiss, onCreateUser} = this.props
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
      superAdmin: userSuperAdmin.text,
      roles: [
        {
          name: userRole,
          organizationName: userOrganization ? userOrganization.name : null,
          organizatioID: userOrganization ? userOrganization.id : null,
        },
      ],
    }

    onCreateUser(newUser)
    onDismiss()
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
    this.setState({userOrganization: newUserOrganization.text})
  }

  render() {
    const {onDismiss, userRoles, organizations} = this.props
    const {
      userName,
      userProvider,
      userScheme,
      userRole,
      userSuperAdmin,
      userOrganization,
    } = this.state

    const isUserNameEmpty = userName === '' || userName === null
    const isUserProviderEmpty = userProvider === '' || userProvider === null

    return (
      <div className="overlay-technology">
        <div className="manage-orgs-form">
          <div className="manage-orgs-form--header">
            <div className="page-header__left">
              <h1 className="page-header__title">Create User</h1>
            </div>
            <div className="page-header__right">
              <span className="page-header__dismiss" onClick={onDismiss} />
            </div>
          </div>
          <div className="manage-orgs-form--body">
            <div className="manage-orgs-form--new">
              <input
                className="form-control input-sm"
                type="text"
                placeholder="Type user's OAuth Username..."
                value={userName}
                onChange={this.handleInputChange('userName')}
              />
              <input
                className="form-control input-sm"
                type="text"
                placeholder="Type user's OAuth Provider..."
                value={userProvider}
                onChange={this.handleInputChange('userProvider')}
              />
              <input
                className="form-control input-sm disabled"
                type="text"
                disabled={true}
                placeholder="Type user's OAuth Scheme..."
                value={userScheme}
              />
              <Dropdown
                items={userRoles.map(role => ({...role, text: role.name}))}
                selected={userRole}
                onChoose={this.handleSelectRole}
              />
              <Authorized requiredRole={SUPERADMIN_ROLE}>
                <Dropdown
                  items={superAdminItems}
                  selected={userSuperAdmin.text}
                  onChoose={this.handleSelectSuperAdmin}
                />
              </Authorized>
              <Authorized
                requiredRole={SUPERADMIN_ROLE}
                replaceWith={
                  <input
                    type="text"
                    value="currentOrganization"
                    disabled={true}
                    className="form-control input-sm disabled"
                  />
                }
              >
                <Dropdown
                  items={organizations.map(org => ({...org, text: org.name}))}
                  selected={userOrganization || 'Add to Organization'}
                  onChoose={this.handleSelectOrganization}
                />
              </Authorized>
            </div>
            <div className="manage-orgs-form--footer">
              <button className="btn btn-sm btn-default" onClick={onDismiss}>
                Cancel
              </button>
              <button
                className="btn btn-sm btn-success"
                onClick={this.handleClickCreateUser}
                disabled={isUserNameEmpty || isUserProviderEmpty}
              >
                Create
              </button>
            </div>
          </div>
        </div>
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

CreateUserOverlay.propTypes = {
  onDismiss: func.isRequired,
  onCreateUser: func.isRequired,
  userRoles: arrayOf(shape()).isRequired,
  organizations: arrayOf(
    shape({
      id: string.isRequired,
      name: string.isRequired,
    })
  ).isRequired,
}
export default CreateUserOverlay
