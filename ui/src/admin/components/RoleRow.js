import React, {PropTypes} from 'react'
import _ from 'lodash'

import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'
import DeleteRow from 'src/admin/components/DeleteRow'

// TODO: replace with permissions list from server
const ALL_PERMISSIONS = [
  "NoPermissions",
  "ViewAdmin",
  "ViewChronograf",
  "CreateDatabase",
  "CreateUserAndRole",
  "AddRemoveNode",
  "DropDatabase",
  "DropData",
  "ReadData",
  "WriteData",
  "Rebalance",
  "ManageShard",
  "ManageContinuousQuery",
  "ManageQuery",
  "ManageSubscription",
  "Monitor",
  "CopyShard",
  "KapacitorAPI",
  "KapacitorConfigAPI",
]

const RoleRow = ({
  role: {name, permissions, users},
  role,
  allUsers,
  onDelete,
  onUpdateRoleUsers,
  onUpdateRolePermissions,
}) => {
  const handleUpdateUsers = (u) => {
    onUpdateRoleUsers(role, u.map((n) => ({name: n})))
  }

  const handleUpdatePermissions = (allowed) => {
    onUpdateRolePermissions(role, [{scope: 'all', allowed}])
  }

  const perms = _.get(permissions, ['0', 'allowed'], [])

  return (
    <tr>
      <td>{name}</td>
      <td>
        {
          permissions && permissions.length ?
            <MultiSelectDropdown
              items={ALL_PERMISSIONS}
              selectedItems={perms}
              label={perms.length ? '' : 'Select Permissions'}
              onApply={handleUpdatePermissions}
            /> : '\u2014'
        }
      </td>
      <td>
        {
          allUsers && allUsers.length ?
            <MultiSelectDropdown
              items={allUsers.map((u) => u.name)}
              selectedItems={users.map((u) => u.name)}
              label={users.length ? '' : 'Select Users'}
              onApply={handleUpdateUsers}
            /> : '\u2014'
        }
      </td>
      <td className="text-right" style={{width: "85px"}}>
        <DeleteRow onDelete={onDelete} item={role} />
      </td>
    </tr>
  )
}

const {
  arrayOf,
  func,
  shape,
  string,
} = PropTypes

RoleRow.propTypes = {
  role: shape({
    name: string,
    permissions: arrayOf(shape({
      name: string,
    })),
    users: arrayOf(shape({
      name: string,
    })),
  }).isRequired,
  onDelete: func.isRequired,
  allUsers: arrayOf(shape()),
  onUpdateRoleUsers: func.isRequired,
  onUpdateRolePermissions: func.isRequired,
}

export default RoleRow
