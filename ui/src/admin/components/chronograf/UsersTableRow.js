import React, {PropTypes} from 'react'

import UsersTableRoleCell from 'src/admin/components/chronograf/UsersTableRoleCell'
import UsersTableOrgCell from 'src/admin/components/chronograf/UsersTableOrgCell'

import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'

const UsersTableRow = ({
  user,
  organizationName,
  onToggleUserSelected,
  selectedUsers,
  isSameUser,
  onChangeUserRole,
  onChooseFilter,
}) => {
  const {colOrg, colRole, colSuperAdmin, colProvider, colScheme} = USERS_TABLE

  const isSelected = selectedUsers.find(u => isSameUser(user, u))

  return (
    <tr className={isSelected ? 'selected' : null}>
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
        <UsersTableOrgCell
          user={user}
          organizationName={organizationName}
          onChangeUserRole={onChangeUserRole}
          onChooseFilter={onChooseFilter}
        />
      </td>
      <td style={{width: colRole}}>
        <UsersTableRoleCell
          user={user}
          organizationName={organizationName}
          onChangeUserRole={onChangeUserRole}
        />
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
}

const {arrayOf, func, shape, string} = PropTypes

UsersTableRow.propTypes = {
  user: shape(),
  organizationName: string.isRequired,
  onToggleUserSelected: func.isRequired,
  selectedUsers: arrayOf(shape()),
  isSameUser: func.isRequired,
  onChangeUserRole: func.isRequired,
  onChooseFilter: func.isRequired,
}

export default UsersTableRow
