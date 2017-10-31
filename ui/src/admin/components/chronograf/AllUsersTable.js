import React, {Component, PropTypes} from 'react'

import _ from 'lodash'

import {DEFAULT_ORG, NO_ORG} from 'src/admin/constants/dummyUsers'

class ChronografAllUsersTable extends Component {
  constructor(props) {
    super(props)
  }

  handleChooseFilter = filterString => () => {
    this.props.onFilterUsers({name: filterString})
  }

  renderOrgAndRoleCell = user => {
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
      return user.roles.map(
        (role, r) =>
          role.organizationName === DEFAULT_ORG
            ? null // don't show Default Organization among user roles
            : <span key={r} className="org-and-role">
                <a
                  href="#"
                  onClick={this.handleChooseFilter(role.organizationName)}
                >
                  {role.organizationName}
                </a>
                <i>({role.name}</i>)
              </span>
      )
    }
    return user.roles
      .filter(role => role.organizationName === organizationName)
      .map((role, r) =>
        <span key={r} className="org-and-role">
          {role.name}
        </span>
      )
  }

  renderTableRows = filteredUsers => {
    const {onToggleUserSelected, selectedUsers, isSameUser} = this.props

    return filteredUsers.map((user, i) => {
      const isSelected = selectedUsers.find(u => isSameUser(user, u))
      return (
        <tr key={i} className={isSelected ? 'selected' : null}>
          <td>
            <div
              className="user-checkbox"
              onClick={onToggleUserSelected(user)}
            />
          </td>
          <td>
            <strong>
              {user.name}
            </strong>
          </td>
          <td>
            {user.superadmin ? 'Yes' : '--'}
          </td>
          <td>
            {this.renderOrgAndRoleCell(user)}
          </td>
          <td>
            {user.provider}
          </td>
          <td className="text-right">
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
    const {
      filteredUsers,
      organizationName,
      onToggleAllUsersSelected,
      selectedUsers,
    } = this.props

    const areAllSelected = this.areSameUsers(filteredUsers, selectedUsers)

    return (
      <table className="table table-highlight chronograf-admin-table">
        <thead>
          <tr>
            <th>
              <div
                className={
                  areAllSelected ? 'user-checkbox selected' : 'user-checkbox'
                }
                onClick={onToggleAllUsersSelected(areAllSelected)}
              />
            </th>
            <th>Username</th>
            <th>SuperAdmin</th>
            <th>
              {organizationName ? 'Role' : 'Organization & Role'}
            </th>
            <th>Provider</th>
            <th className="text-right">Scheme</th>
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
}
export default ChronografAllUsersTable
