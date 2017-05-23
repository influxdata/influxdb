import React, {PropTypes} from 'react'

import _ from 'lodash'

import UserEditingRow from 'src/admin/components/UserEditingRow'
import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'
import ConfirmButtons from 'shared/components/ConfirmButtons'
import DeleteConfirmTableCell from 'shared/components/DeleteConfirmTableCell'
import ChangePassRow from 'src/admin/components/ChangePassRow'

const UserRow = ({
  user: {name, roles, permissions, password},
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
  onUpdatePermissions,
  onUpdateRoles,
  onUpdatePassword,
}) => {
  const handleUpdatePermissions = allowed => {
    onUpdatePermissions(user, [{scope: 'all', allowed}])
  }

  const handleUpdateRoles = roleNames => {
    onUpdateRoles(
      user,
      allRoles.filter(r => roleNames.find(rn => rn === r.name))
    )
  }

  const handleUpdatePassword = () => {
    onUpdatePassword(user, password)
  }

  if (isEditing) {
    return (
      <tr className="admin-table--edit-row">
        <UserEditingRow
          user={user}
          onEdit={onEdit}
          onSave={onSave}
          isNew={isNew}
        />
        {hasRoles ? <td>--</td> : null}
        <td>--</td>
        <td style={{width: '190px'}} />
        <td className="text-right" style={{width: '85px'}}>
          <ConfirmButtons item={user} onConfirm={onSave} onCancel={onCancel} />
        </td>
      </tr>
    )
  }

  return (
    <tr>
      <td style={{width: '240px'}}>{name}</td>
      {hasRoles
        ? <td>
            <MultiSelectDropdown
              items={allRoles.map(r => r.name)}
              selectedItems={
                roles
                  ? roles.map(r => r.name)
                  : [] /* TODO remove check when server returns empty list */
              }
              label={roles && roles.length ? '' : 'Select Roles'}
              onApply={handleUpdateRoles}
            />
          </td>
        : null}

      <td>
        {allPermissions && allPermissions.length
          ? <MultiSelectDropdown
              items={allPermissions}
              selectedItems={_.get(permissions, ['0', 'allowed'], [])}
              label={
                permissions && permissions.length ? '' : 'Select Permissions'
              }
              onApply={handleUpdatePermissions}
            />
          : null}
      </td>
      <td className="text-right" style={{width: '190px'}}>
        <ChangePassRow
          onEdit={onEdit}
          onApply={handleUpdatePassword}
          user={user}
          buttonSize="btn-xs"
        />
      </td>
      <DeleteConfirmTableCell
        onDelete={onDelete}
        item={user}
        buttonSize="btn-xs"
      />
    </tr>
  )
}

const {arrayOf, bool, func, shape, string} = PropTypes

UserRow.propTypes = {
  user: shape({
    name: string,
    roles: arrayOf(
      shape({
        name: string,
      })
    ),
    permissions: arrayOf(
      shape({
        name: string,
      })
    ),
    password: string,
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
  onUpdatePermissions: func,
  onUpdateRoles: func,
  onUpdatePassword: func,
}

export default UserRow
