import React, {PropTypes} from 'react'
import _ from 'lodash'

import RoleEditingRow from 'src/admin/components/RoleEditingRow'
import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'
import ConfirmButtons from 'src/admin/components/ConfirmButtons'
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
  isNew,
  isEditing,
  onEdit,
  onSave,
  onCancel,
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
      {
        isEditing ?
          <RoleEditingRow role={role} onEdit={onEdit} onSave={onSave} isNew={isNew} /> :
          <td>{name}</td>
      }
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
              selectedItems={users === undefined ? [] : users.map((u) => u.name)}
              label={users && users.length ? '' : 'Select Users'}
              onApply={handleUpdateUsers}
            /> : '\u2014'
        }
      </td>
      <td className="text-right" style={{width: "85px"}}>
        {
          isEditing ?
            <ConfirmButtons item={role} onConfirm={onSave} onCancel={onCancel} /> :
            <DeleteRow onDelete={onDelete} item={role} />
        }
      </td>
    </tr>
  )
}

const {
  arrayOf,
  bool,
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
  isNew: bool,
  isEditing: bool,
  onCancel: func,
  onEdit: func,
  onSave: func,
  onDelete: func.isRequired,
  allUsers: arrayOf(shape()),
  onUpdateRoleUsers: func.isRequired,
  onUpdateRolePermissions: func.isRequired,
}

export default RoleRow
