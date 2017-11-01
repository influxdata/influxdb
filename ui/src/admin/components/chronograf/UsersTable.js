import React, {Component, PropTypes} from 'react'

import _ from 'lodash'

import UsersTableRow from 'src/admin/components/chronograf/UsersTableRow'
import OrgTableRow from 'src/admin/components/chronograf/OrgTableRow'

import {DEFAULT_ORG} from 'src/admin/constants/dummyUsers'
import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'

class ChronografUsersTable extends Component {
  constructor(props) {
    super(props)
  }

  handleChooseFilter = filterString => () => {
    this.props.onFilterUsers({name: filterString})
  }

  handleChangeUserRole = (user, currentRole) => newRole => {
    this.props.onUpdateUserRole(user, currentRole, newRole)
  }

  areSameUsers = (usersA, usersB) => {
    const {isSameUser} = this.props
    return !_.differenceWith(usersA, usersB, isSameUser).length
  }

  render() {
    const {
      organizationName,
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
            <th style={{width: colSuperAdmin}}>SuperAdmin</th>
            <th style={{width: colProvider}}>Provider</th>
            <th className="text-right" style={{width: colScheme}}>
              Scheme
            </th>
          </tr>
        </thead>
        <tbody>
          {filteredUsers.map(
            (user, i) =>
              organizationName === DEFAULT_ORG
                ? <UsersTableRow
                    user={user}
                    key={i}
                    onToggleUserSelected={onToggleUserSelected}
                    selectedUsers={selectedUsers}
                    isSameUser={isSameUser}
                    onChangeUserRole={this.handleChangeUserRole}
                    onChooseFilter={this.handleChooseFilter}
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
          )}
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
  onUpdateUserRole: func.isRequired,
}
export default ChronografUsersTable
