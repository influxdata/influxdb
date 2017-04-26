import React, {PropTypes} from 'react'

import _ from 'lodash'

import RoleEditingRow from 'src/admin/components/RoleEditingRow'
import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'
import ConfirmButtons from 'shared/components/ConfirmButtons'
import DeleteConfirmTableCell from 'shared/components/DeleteConfirmTableCell'

const RoleRow = ({
  role: {name, permissions, users},
  role,
  allUsers,
  allPermissions,
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

  if (isEditing) {
    return (
      <tr className="admin-table--edit-row">
        <RoleEditingRow role={role} onEdit={onEdit} onSave={onSave} isNew={isNew} />
        <td></td>
        <td></td>
        <td className="text-right" style={{width: '85px'}}>
          <ConfirmButtons item={role} onConfirm={onSave} onCancel={onCancel} />
        </td>
      </tr>
    )
  }

  return (
    <tr>
      <td>{name}</td>
      <td>
        {
          allPermissions && allPermissions.length ?
            <MultiSelectDropdown
              items={allPermissions}
              selectedItems={perms}
              label={perms.length ? '' : 'Select Permissions'}
              onApply={handleUpdatePermissions}
            /> : null
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
            /> : null
        }
      </td>
      <DeleteConfirmTableCell onDelete={onDelete} item={role} />
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
  allPermissions: arrayOf(string),
  onUpdateRoleUsers: func.isRequired,
  onUpdateRolePermissions: func.isRequired,
}

export default RoleRow
