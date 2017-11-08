import React, {Component, PropTypes} from 'react'

import _ from 'lodash'

import Authorized, {SUPERADMIN_ROLE} from 'src/auth/Authorized'

import UsersTableRow from 'src/admin/components/chronograf/UsersTableRow'
import OrgTableRow from 'src/admin/components/chronograf/OrgTableRow'
import NewUserTableRow from 'src/admin/components/chronograf/NewUserTableRow'

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

  handleChangeSuperAdmin = (user, currentStatus) => newStatus => {
    this.props.onUpdateUserSuperAdmin(user, currentStatus, newStatus)
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
      userRoles,
      organization,
      organizations,
      filteredUsers,
      onToggleAllUsersSelected,
      onToggleUserSelected,
      selectedUsers,
      isSameUser,
      isCreatingUser,
      currentOrganization,
      onBlurCreateUserRow,
      onCreateUser,
    } = this.props
    const {
      colOrg,
      colRole,
      colSuperAdmin,
      colProvider,
      colScheme,
      colActions,
    } = USERS_TABLE

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
            <th style={{width: colOrg}} className="align-with-col-text">
              Organization
            </th>
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
                currentOrganization={currentOrganization}
                organizations={organizations}
                roles={userRoles}
                onBlur={onBlurCreateUserRow}
                onCreateUser={onCreateUser}
              />
            : null}
          {filteredUsers.length
            ? filteredUsers.map(
                (user, i) =>
                  organization.name === DEFAULT_ORG_NAME
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
                        organization={organization}
                        onSelectAddUserToOrg={this.handleSelectAddUserToOrg(
                          user
                        )}
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

const {arrayOf, bool, func, shape, string} = PropTypes

ChronografUsersTable.propTypes = {
  userRoles: arrayOf(shape()).isRequired,
  filteredUsers: arrayOf(shape()),
  selectedUsers: arrayOf(shape()),
  onFilterUsers: func.isRequired,
  onToggleUserSelected: func.isRequired,
  onToggleAllUsersSelected: func.isRequired,
  isSameUser: func.isRequired,
  isCreatingUser: bool,
  organization: shape({
    name: string.isRequired,
    id: string.isRequired,
  }),
  organizations: arrayOf(
    shape({
      id: string.isRequired,
      name: string.isRequired,
    })
  ),
  onAddUserToOrg: func.isRequired,
  onUpdateUserRole: func.isRequired,
  onCreateUser: func.isRequired,
  onUpdateUserSuperAdmin: func.isRequired,
  onBlurCreateUserRow: func.isRequired,
  currentOrganization: shape({
    id: string.isRequired,
    name: string.isRequired,
  }).isRequired,
}
export default ChronografUsersTable
