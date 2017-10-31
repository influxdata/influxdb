import React, {Component, PropTypes} from 'react'

import _ from 'lodash'

import Dropdown from 'shared/components/Dropdown'

import {DEFAULT_ORG, NO_ORG, USER_ROLES} from 'src/admin/constants/dummyUsers'
import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'

class ChronografAllUsersTable extends Component {
  constructor(props) {
    super(props)
  }

  handleChooseFilter = filterString => () => {
    this.props.onFilterUsers({name: filterString})
  }

  handleChangeUserRole = (user, currentRole) => newRole => {
    this.props.onUpdateUserRole(user, currentRole, newRole)
  }

  renderOrgCell = user => {
    const {organizationName} = this.props

    // Expects Users to always have at least 1 role (as a member of the default org)
    if (user.roles.length === 1) {
      return (
        <a href="#" onClick={this.handleChooseFilter(NO_ORG)}>
          {NO_ORG}
        </a>
      )
    }

    if (organizationName === DEFAULT_ORG) {
      return user.roles
        .filter(role => {
          return !(role.organizationName === DEFAULT_ORG)
        })
        .map((role, i) =>
          <span key={i} className="chronograf-user--org">
            <a
              href="#"
              onClick={this.handleChooseFilter(role.organizationName)}
            >
              {role.organizationName}
            </a>
          </span>
        )
    }

    const currentOrg = user.roles.find(
      role => role.organizationName === organizationName
    )
    return (
      <span className="chronograf-user--org">
        <a
          href="#"
          onClick={this.handleChooseFilter(currentOrg.organizationName)}
        >
          {currentOrg.organizationName}
        </a>
      </span>
    )
  }

  renderRoleCell = user => {
    const {organizationName} = this.props

    // Expects Users to always have at least 1 role (as a member of the default org)
    if (user.roles.length === 1) {
      return <span className="chronograf-user--role">No Role</span>
    }

    if (organizationName === DEFAULT_ORG) {
      return user.roles
        .filter(role => {
          return !(role.organizationName === DEFAULT_ORG)
        })
        .map((role, i) =>
          <Dropdown
            key={i}
            items={USER_ROLES.map(r => ({
              ...r,
              text: r.name,
            }))}
            selected={role.name}
            onChoose={this.handleChangeUserRole(user, role)}
            buttonColor="btn-primary"
            className="dropdown-140"
          />
        )
    }

    const currentOrg = user.roles.find(
      role => role.organizationName === organizationName
    )
    return (
      <span className="chronograf-user--role">
        {currentOrg.name}
      </span>
    )
  }

  renderTableRows = filteredUsers => {
    const {colOrg, colRole, colSuperAdmin, colProvider, colScheme} = USERS_TABLE
    const {onToggleUserSelected, selectedUsers, isSameUser} = this.props

    return filteredUsers.map((user, i) => {
      const isSelected = selectedUsers.find(u => isSameUser(user, u))
      return (
        <tr key={i} className={isSelected ? 'selected' : null}>
          <td
            onClick={onToggleUserSelected(user)}
            className="chronograf-admin-table--check-col chronograf-admin-table--selectable"
          >
            <div className="user-checkbox" />
          </td>
          <td
            onClick={onToggleUserSelected(user)}
            className="chronograf-admin-table--selectable"
          >
            <strong>
              {user.name}
            </strong>
          </td>
          <td style={{width: colOrg}}>
            {this.renderOrgCell(user)}
          </td>
          <td style={{width: colRole}}>
            {this.renderRoleCell(user)}
          </td>
          <td style={{width: colSuperAdmin}}>
            {user.superadmin ? 'Yes' : '--'}
          </td>
          <td style={{width: colProvider}}>
            {user.provider}
          </td>
          <td className="text-right" style={{width: colScheme}}>
            {user.scheme}
          </td>
        </tr>
      )
    })
  }

  areSameUsers = (usersA, usersB) => {
    const {isSameUser} = this.props
    return !_.differenceWith(usersA, usersB, isSameUser).length
  }

  render() {
    const {filteredUsers, onToggleAllUsersSelected, selectedUsers} = this.props
    const {colOrg, colRole, colSuperAdmin, colProvider, colScheme} = USERS_TABLE

    const areAllSelected = this.areSameUsers(filteredUsers, selectedUsers)

    return (
      <table className="table table-highlight chronograf-admin-table">
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
          {this.renderTableRows(filteredUsers)}
        </tbody>
      </table>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

ChronografAllUsersTable.propTypes = {
  filteredUsers: arrayOf(shape),
  selectedUsers: arrayOf(shape),
  onFilterUsers: func.isRequired,
  onToggleUserSelected: func.isRequired,
  onToggleAllUsersSelected: func.isRequired,
  isSameUser: func.isRequired,
  organizationName: string,
  onUpdateUserRole: func.isRequired,
}
export default ChronografAllUsersTable
