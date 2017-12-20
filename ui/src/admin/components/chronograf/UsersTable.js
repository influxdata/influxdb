import React, {Component, PropTypes} from 'react'

import uuid from 'node-uuid'

import Authorized, {SUPERADMIN_ROLE} from 'src/auth/Authorized'

import UsersTableHeader from 'src/admin/components/chronograf/UsersTableHeader'
import UsersTableRowNew from 'src/admin/components/chronograf/UsersTableRowNew'
import UsersTableRow from 'src/admin/components/chronograf/UsersTableRow'

import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'

class UsersTable extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isCreatingUser: false,
    }
  }

  handleChangeUserRole = (user, currentRole) => newRole => {
    this.props.onUpdateUserRole(user, currentRole, newRole)
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
    const {organization, users, onCreateUser, meID, notify} = this.props

    const {isCreatingUser} = this.state
    const {
      colRole,
      colSuperAdmin,
      colProvider,
      colScheme,
      colActions,
    } = USERS_TABLE

    return (
      <div className="panel panel-default">
        <UsersTableHeader
          numUsers={users.length}
          onClickCreateUser={this.handleClickCreateUser}
          isCreatingUser={isCreatingUser}
          organization={organization}
        />
        <div className="panel-body">
          <table className="table table-highlight v-center chronograf-admin-table">
            <thead>
              <tr>
                <th>Username</th>
                <th style={{width: colRole}} className="align-with-col-text">
                  Role
                </th>
                <Authorized requiredRole={SUPERADMIN_ROLE}>
                  <th style={{width: colSuperAdmin}} className="text-center">
                    SuperAdmin
                  </th>
                </Authorized>
                <th style={{width: colProvider}}>Provider</th>
                <th style={{width: colScheme}}>Scheme</th>
                <th className="text-right" style={{width: colActions}} />
              </tr>
            </thead>
            <tbody>
              {isCreatingUser
                ? <UsersTableRowNew
                    organization={organization}
                    onBlur={this.handleBlurCreateUserRow}
                    onCreateUser={onCreateUser}
                    notify={notify}
                  />
                : null}
              {users.length || !isCreatingUser
                ? users.map(user =>
                    <UsersTableRow
                      user={user}
                      key={uuid.v4()}
                      organization={organization}
                      onChangeUserRole={this.handleChangeUserRole}
                      onChangeSuperAdmin={this.handleChangeSuperAdmin}
                      onDelete={this.handleDeleteUser}
                      meID={meID}
                    />
                  )
                : <tr className="table-empty-state">
                    <Authorized
                      requiredRole={SUPERADMIN_ROLE}
                      replaceWithIfNotAuthorized={
                        <th colSpan="5">
                          <p>No Users to display</p>
                        </th>
                      }
                    >
                      <th colSpan="6">
                        <p>No Users to display</p>
                      </th>
                    </Authorized>
                  </tr>}
            </tbody>
          </table>
        </div>
      </div>
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

UsersTable.propTypes = {
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
  organization: shape({
    name: string.isRequired,
    id: string.isRequired,
  }),
  onCreateUser: func.isRequired,
  onUpdateUserRole: func.isRequired,
  onUpdateUserSuperAdmin: func.isRequired,
  onDeleteUser: func.isRequired,
  meID: string.isRequired,
  notify: func.isRequired,
}

export default UsersTable
