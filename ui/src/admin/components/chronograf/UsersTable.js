import React, {Component, PropTypes} from 'react'

import _ from 'lodash'

import Authorized, {SUPERADMIN_ROLE} from 'src/auth/Authorized'

import UsersTableHeader from 'src/admin/components/chronograf/UsersTableHeader'
import OrgTableRow from 'src/admin/components/chronograf/OrgTableRow'
import NewUserTableRow from 'src/admin/components/chronograf/NewUserTableRow'
import BatchActionsBar from 'src/admin/components/chronograf/BatchActionsBar'

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

  handleChangeSuperAdmin = (user, currentStatus) => newStatus => {
    this.props.onUpdateUserSuperAdmin(user, currentStatus, newStatus)
  }

  handleClickCreateUserRow = () => {
    this.setState({isCreatingUser: true})
  }

  handleBlurCreateUserRow = () => {
    this.setState({isCreatingUser: false})
  }

  areSameUsers = (usersA, usersB) => {
    if (usersA.length === 0 && usersB.length === 0) {
      return false
    }
    const {isSameUser} = this.props
    return !_.differenceWith(usersA, usersB, isSameUser).length
  }

  render() {
    const {
      organization,
      users,
      onToggleAllUsersSelected,
      onToggleUserSelected,
      selectedUsers,
      isSameUser,
      onCreateUser,
      onDeleteUsers,
      onChangeRoles,
    } = this.props

    const {isCreatingUser} = this.state
    const {
      colRole,
      colSuperAdmin,
      colProvider,
      colScheme,
      colActions,
    } = USERS_TABLE

    const areAllSelected = this.areSameUsers(users, selectedUsers)

    return (
      <div className="panel panel-minimal">
        <UsersTableHeader
          numUsers={users.length}
          onCreateUserRow={this.handleClickCreateUserRow}
        />
        <BatchActionsBar
          numUsersSelected={selectedUsers.length}
          onDeleteUsers={onDeleteUsers}
          onChangeRoles={onChangeRoles}
        />
        <div className="panel-body">
          <table className="table table-highlight v-center chronograf-admin-table">
            <thead>
              <tr>
                <th className="chronograf-admin-table--check-col">
                  <div
                    className={
                      areAllSelected
                        ? 'user-checkbox selected'
                        : 'user-checkbox'
                    }
                    onClick={onToggleAllUsersSelected(areAllSelected)}
                  />
                </th>
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
                ? <NewUserTableRow
                    organization={organization}
                    onBlur={this.handleBlurCreateUserRow}
                    onCreateUser={onCreateUser}
                  />
                : null}
              {users.length || !isCreatingUser
                ? users.map((user, i) =>
                    <OrgTableRow
                      user={user}
                      key={i}
                      onToggleUserSelected={onToggleUserSelected}
                      selectedUsers={selectedUsers}
                      isSameUser={isSameUser}
                      organization={organization}
                      onChangeUserRole={this.handleChangeUserRole}
                      onChangeSuperAdmin={this.handleChangeSuperAdmin}
                    />
                  )
                : <tr className="table-empty-state">
                    <Authorized
                      requiredRole={SUPERADMIN_ROLE}
                      replaceWith={
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

const {arrayOf, func, shape, string} = PropTypes

UsersTable.propTypes = {
  users: arrayOf(shape()),
  selectedUsers: arrayOf(shape()),
  onToggleUserSelected: func.isRequired,
  onToggleAllUsersSelected: func.isRequired,
  isSameUser: func.isRequired,
  organization: shape({
    name: string.isRequired,
    id: string.isRequired,
  }),
  onUpdateUserRole: func.isRequired,
  onCreateUser: func.isRequired,
  onUpdateUserSuperAdmin: func.isRequired,
  onDeleteUsers: func.isRequired,
  onChangeRoles: func.isRequired,
}
export default UsersTable
