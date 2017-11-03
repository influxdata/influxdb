import React, {PropTypes} from 'react'

import Authorized, {SUPERADMIN_ROLE} from 'src/auth/Authorized'

import UsersTableRoleCell from 'src/admin/components/chronograf/UsersTableRoleCell'
import UsersTableOrgCell from 'src/admin/components/chronograf/UsersTableOrgCell'
import UsersTableSuperAdminCell from 'src/admin/components/chronograf/UsersTableSuperAdminCell'

import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'

const UsersTableRow = ({
  user,
  organizations,
  onToggleUserSelected,
  selectedUsers,
  isSameUser,
  onChangeUserRole,
  onChooseFilter,
  onChangeSuperAdmin,
}) => {
  const {colProvider, colScheme} = USERS_TABLE

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
      <UsersTableOrgCell
        user={user}
        organizations={organizations}
        onChangeUserRole={onChangeUserRole}
        onChooseFilter={onChooseFilter}
      />
      <UsersTableRoleCell user={user} onChangeUserRole={onChangeUserRole} />
      <Authorized requiredRole={SUPERADMIN_ROLE}>
        <UsersTableSuperAdminCell
          superAdmin={user.role === SUPERADMIN_ROLE}
          user={user}
          onChangeSuperAdmin={onChangeSuperAdmin}
        />
      </Authorized>
      <td style={{width: colProvider}}>
        {user.provider}
      </td>
      <td className="text-right" style={{width: colScheme}}>
        {user.scheme}
      </td>
    </tr>
  )
}

const {arrayOf, func, shape} = PropTypes

UsersTableRow.propTypes = {
  user: shape(),
  organizations: arrayOf(shape),
  onToggleUserSelected: func.isRequired,
  selectedUsers: arrayOf(shape()),
  isSameUser: func.isRequired,
  onChangeUserRole: func.isRequired,
  onChooseFilter: func.isRequired,
  onChangeSuperAdmin: func.isRequired,
}

export default UsersTableRow
