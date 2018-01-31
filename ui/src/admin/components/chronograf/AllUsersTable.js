import React, {Component, PropTypes} from 'react'

import uuid from 'node-uuid'

import AllUsersTableHeader from 'src/admin/components/chronograf/AllUsersTableHeader'
import AllUsersTableRowNew from 'src/admin/components/chronograf/AllUsersTableRowNew'
import AllUsersTableRow from 'src/admin/components/chronograf/AllUsersTableRow'

import {ALL_USERS_TABLE} from 'src/admin/constants/chronografTableSizing'
const {
  colOrganizations,
  colProvider,
  colScheme,
  colSuperAdmin,
  colActions,
} = ALL_USERS_TABLE

class AllUsersTable extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isCreatingUser: false,
    }
  }

  handleUpdateAuthConfig = fieldName => updatedValue => {
    const {
      actionsConfig: {updateAuthConfigAsync},
      authConfig,
      links,
    } = this.props
    const updatedAuthConfig = {
      ...authConfig,
      [fieldName]: updatedValue,
    }
    updateAuthConfigAsync(links.config.auth, authConfig, updatedAuthConfig)
  }

  handleAddToOrganization = user => organization => {
    // '*' tells the server to fill in the current defaultRole of that org
    const newRoles = user.roles.concat({
      organization: organization.id,
      name: '*',
    })
    this.props.onUpdateUserRoles(
      user,
      newRoles,
      `${user.name} has been added to ${organization.name}`
    )
  }

  handleRemoveFromOrganization = user => role => {
    const newRoles = user.roles.filter(
      r => r.organization !== role.organization // TODO: the organization upstream of this should use .id instead of .organization for this property
    )
    const {name} = this.props.organizations.find(
      o => o.id === role.organization
    )
    this.props.onUpdateUserRoles(
      user,
      newRoles,
      `${user.name} has been removed from ${name}`
    )
  }

  handleChangeSuperAdmin = user => newStatus => {
    this.props.onUpdateUserSuperAdmin(user, newStatus)
  }

  handleDeleteUser = user => {
    this.props.onDeleteUser(user)
  }

  handleClickCreateUser = () => {
    this.setState({isCreatingUser: true})
  }

  handleBlurCreateUserRow = () => {
    this.setState({isCreatingUser: false})
  }

  render() {
    const {
      users,
      organizations,
      onCreateUser,
      authConfig,
      meID,
      notify,
    } = this.props

    const {isCreatingUser} = this.state

    return (
      <div className="panel panel-default">
        <AllUsersTableHeader
          numUsers={users.length}
          numOrganizations={organizations.length}
          onClickCreateUser={this.handleClickCreateUser}
          isCreatingUser={isCreatingUser}
          authConfig={authConfig}
          onChangeAuthConfig={this.handleUpdateAuthConfig}
        />
        <div className="panel-body">
          <table className="table table-highlight v-center chronograf-admin-table">
            <thead>
              <tr>
                <th>Username</th>
                <th
                  style={{width: colOrganizations}}
                  className="align-with-col-text"
                >
                  Organizations
                </th>
                <th style={{width: colProvider}}>Provider</th>
                <th style={{width: colScheme}}>Scheme</th>
                <th style={{width: colSuperAdmin}} className="text-center">
                  SuperAdmin
                </th>
                <th className="text-right" style={{width: colActions}} />
              </tr>
            </thead>
            <tbody>
              {isCreatingUser
                ? <AllUsersTableRowNew
                    organizations={organizations}
                    onBlur={this.handleBlurCreateUserRow}
                    onCreateUser={onCreateUser}
                    notify={notify}
                  />
                : null}
              {users.length || !isCreatingUser
                ? users.map(user =>
                    <AllUsersTableRow
                      user={user}
                      key={uuid.v4()}
                      organizations={organizations}
                      onAddToOrganization={this.handleAddToOrganization}
                      onRemoveFromOrganization={
                        this.handleRemoveFromOrganization
                      }
                      onChangeSuperAdmin={this.handleChangeSuperAdmin}
                      onDelete={this.handleDeleteUser}
                      meID={meID}
                    />
                  )
                : <tr className="table-empty-state">
                    <th colSpan="6">
                      <p>No Users to display</p>
                    </th>
                  </tr>}
            </tbody>
          </table>
        </div>
      </div>
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

AllUsersTable.propTypes = {
  links: shape({
    config: shape({
      auth: string.isRequired,
    }).isRequired,
  }).isRequired,
  users: arrayOf(
    shape({
      id: string,
      links: shape({
        self: string.isRequired,
      }),
      name: string.isRequired,
      provider: string.isRequired,
      roles: arrayOf(
        shape({
          name: string.isRequired,
          organization: string.isRequired,
        })
      ),
      scheme: string.isRequired,
      superAdmin: bool,
    })
  ).isRequired,
  organizations: arrayOf(
    shape({
      name: string.isRequired,
      id: string.isRequired,
    })
  ),
  onCreateUser: func.isRequired,
  onUpdateUserRoles: func.isRequired,
  onUpdateUserSuperAdmin: func.isRequired,
  onDeleteUser: func.isRequired,
  actionsConfig: shape({
    getAuthConfigAsync: func.isRequired,
    updateAuthConfigAsync: func.isRequired,
  }),
  authConfig: shape({
    superAdminNewUsers: bool,
  }),
  meID: string.isRequired,
  notify: func.isRequired,
}

export default AllUsersTable
