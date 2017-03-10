import React, {PropTypes} from 'react'

import _ from 'lodash'

import UserEditingRow from 'src/admin/components/UserEditingRow'
import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'
import ConfirmButtons from 'src/admin/components/ConfirmButtons'
import DeleteRow from 'src/admin/components/DeleteRow'

const UserRow = ({
  user: {name, roles, permissions},
  user,
  allRoles,
  allPermissions,
  hasRoles,
  isNew,
  isEditing,
  onEdit,
  onSave,
  onCancel,
  onDelete,
}) => {
  if (isEditing) {
    return (
      <tr className="admin-table--edit-row">
        <UserEditingRow user={user} onEdit={onEdit} onSave={onSave} isNew={isNew} />
        {hasRoles ? <td></td> : null}
        <td></td>
        <td className="text-right" style={{width: "85px"}}>
          <ConfirmButtons item={user} onConfirm={onSave} onCancel={onCancel} />
        </td>
      </tr>
    )
  }

  return (
    <tr>
      <td>{name}</td>
      {
        hasRoles ?
          <td>
            <MultiSelectDropdown
              items={allRoles.map((r) => r.name)}
              selectedItems={roles ? roles.map((r) => r.name) : []/* TODO remove check when server returns empty list */}
              label={roles && roles.length ? '' : 'Select Roles'}
              onApply={() => '//TODO'}
            />
          </td> :
          null
      }
      <td>
        {
          allPermissions && allPermissions.length ?
            <MultiSelectDropdown
              items={allPermissions}
              selectedItems={_.get(permissions, ['0', 'allowed'], [])}
              label={permissions && permissions.length ? '' : 'Select Permissions'}
              onApply={() => '//TODO'}
            /> : null
        }
      </td>
      <td className="text-right" style={{width: "85px"}}>
        <DeleteRow onDelete={onDelete} item={user} />
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

UserRow.propTypes = {
  user: shape({
    name: string,
    roles: arrayOf(shape({
      name: string,
    })),
    permissions: arrayOf(shape({
      name: string,
    })),
  }).isRequired,
  allRoles: arrayOf(shape()),
  allPermissions: arrayOf(string),
  hasRoles: bool,
  isNew: bool,
  isEditing: bool,
  onCancel: func,
  onEdit: func,
  onSave: func,
  onDelete: func.isRequired,
}

export default UserRow
