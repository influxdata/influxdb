import React, {Component, PropTypes} from 'react'

import _ from 'lodash'

class ChronografAllUsersTable extends Component {
  constructor(props) {
    super(props)
  }

  renderOrgAndRoleCell = user => {
    const {onFilterUsers, organizationName} = this.props

    if (!user.roles.length) {
      return <a href="#">None</a>
    }

    return organizationName
      ? user.roles
          .filter(role => role.organizationName === organizationName)
          .map((role, r) =>
            <span key={r} className="org-and-role">
              {role.name}
            </span>
          )
      : user.roles.map((role, r) =>
          <span key={r} className="org-and-role">
            <a href="#" onClick={onFilterUsers(role.organizationName)}>
              {role.organizationName}
            </a>
            <i>({role.name}</i>)
          </span>
        )
  }

  renderTableRows = filteredUsers => {
    const {onToggleUserSelected, selectedUsers, isSameUser} = this.props

    return filteredUsers.map((user, i) => {
      const isSelected = selectedUsers.find(u => isSameUser(user, u))
      return (
        <tr key={i}>
          <td>
            <div
              className={isSelected ? 'active' : null}
              onClick={onToggleUserSelected(user)}
            >
              {isSelected ? '[x]' : '[ ]'}
            </div>
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
      <table className="table table-highlight">
        <thead>
          <tr>
            <th>
              <div
                className={areAllSelected ? 'active' : null}
                onClick={onToggleAllUsersSelected(areAllSelected)}
              >
                {areAllSelected ? '[x]' : '[ ]'}
              </div>
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
  filteredUsers: arrayOf(shape()),
  selectedUsers: arrayOf(shape()),
  onFilterUsers: func.isRequired,
  onToggleUserSelected: func.isRequired,
  onToggleAllUsersSelected: func.isRequired,
  isSameUser: func.isRequired,
  organizationName: string,
}
export default ChronografAllUsersTable
