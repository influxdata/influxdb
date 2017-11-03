import React, {Component, PropTypes} from 'react'

import _ from 'lodash'

import Authorized, {SUPERADMIN_ROLE} from 'src/auth/Authorized'

import UsersTableRow from 'src/admin/components/chronograf/UsersTableRow'
import OrgTableRow from 'src/admin/components/chronograf/OrgTableRow'

import {DEFAULT_ORG_NAME} from 'src/admin/constants/dummyUsers'
import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'

class ChronografUsersTable extends Component {
  constructor(props) {
    super(props)
  }

  handleChooseFilter = organization => () => {
    this.props.onFilterUsers({organization})
  }

  handleSelectAddUserToOrg = user => organization => {
    this.props.onAddUserToOrg(user, organization)
  }

  handleChangeUserRole = (user, currentRole) => newRole => {
    this.props.onUpdateUserRole(user, currentRole, newRole)
  }

  handleChangeSuperAdmin = (_user, _currentState) => _newState => {}

  areSameUsers = (usersA, usersB) => {
    if (usersA.length === 0 && usersB.length === 0) {
      return false
    }
    const {isSameUser} = this.props
    return !_.differenceWith(usersA, usersB, isSameUser).length
  }

  render() {
    const {
      organizationName,
      organizations,
      filteredUsers,
      onToggleAllUsersSelected,
      onToggleUserSelected,
      selectedUsers,
      isSameUser,
    } = this.props
    const {colOrg, colRole, colSuperAdmin, colProvider, colScheme} = USERS_TABLE

    const areAllSelected = this.areSameUsers(filteredUsers, selectedUsers)

    return (
      <table className="table table-highlight v-center chronograf-admin-table">
        <thead>
          <tr>
            <th className="chronograf-admin-table--check-col">
              <div
                className={
                  areAllSelected ? 'user-checkbox selected' : 'user-checkbox'
                }
                onClick={onToggleAllUsersSelected(areAllSelected)}
              />
            </th>
            <th>Username</th>
            <th style={{width: colOrg}}>Organization</th>
            <th style={{width: colRole}}>Role</th>
            <Authorized requiredRole={SUPERADMIN_ROLE}>
              <th style={{width: colSuperAdmin}}>SuperAdmin</th>
            </Authorized>
            <th style={{width: colProvider}}>Provider</th>
            <th className="text-right" style={{width: colScheme}}>
              Scheme
            </th>
          </tr>
        </thead>
        <tbody>
          {filteredUsers.length
            ? filteredUsers.map(
                (user, i) =>
                  organizationName === DEFAULT_ORG_NAME
                    ? <UsersTableRow
                        user={user}
                        key={i}
                        organizations={organizations}
                        onToggleUserSelected={onToggleUserSelected}
                        selectedUsers={selectedUsers}
                        isSameUser={isSameUser}
                        onSelectAddUserToOrg={this.handleSelectAddUserToOrg(
                          user
                        )}
                        onChangeUserRole={this.handleChangeUserRole}
                        onChooseFilter={this.handleChooseFilter}
                        onChangeSuperAdmin={this.handleChangeSuperAdmin}
                      />
                    : <OrgTableRow
                        user={user}
                        key={i}
                        onToggleUserSelected={onToggleUserSelected}
                        selectedUsers={selectedUsers}
                        isSameUser={isSameUser}
                        organizationName={organizationName}
                        onChangeUserRole={this.handleChangeUserRole}
                      />
              )
            : <tr className="table-empty-state">
                <Authorized
                  requiredRole={SUPERADMIN_ROLE}
                  replaceWith={
                    <th colSpan="6">
                      <p>No Users to display</p>
                    </th>
                  }
                >
                  <th colSpan="7">
                    <p>No Users to display</p>
                  </th>
                </Authorized>
              </tr>}
        </tbody>
      </table>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

ChronografUsersTable.propTypes = {
  filteredUsers: arrayOf(shape()),
  selectedUsers: arrayOf(shape()),
  onFilterUsers: func.isRequired,
  onToggleUserSelected: func.isRequired,
  onToggleAllUsersSelected: func.isRequired,
  isSameUser: func.isRequired,
  organizationName: string,
  organizations: arrayOf(shape),
  onAddUserToOrg: func.isRequired,
  onUpdateUserRole: func.isRequired,
}
export default ChronografUsersTable
