import React from 'react'
import PropTypes from 'prop-types'

import _ from 'lodash'
import classnames from 'classnames'

import RoleEditingRow from 'src/admin/components/RoleEditingRow'
import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'
import ConfirmOrCancel from 'shared/components/ConfirmOrCancel'
import ConfirmButton from 'shared/components/ConfirmButton'
import {ROLES_TABLE} from 'src/admin/constants/tableSizing'

const RoleRow = ({
  role: {name: roleName, permissions, users = []},
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
  function handleUpdateUsers(usrs) {
    onUpdateRoleUsers(role, usrs)
  }

  function handleUpdatePermissions(allowed) {
    onUpdateRolePermissions(role, [
      {scope: 'all', allowed: allowed.map(({name}) => name)},
    ])
  }

  const perms = _.get(permissions, ['0', 'allowed'], [])

  if (isEditing) {
    return (
      <tr className="admin-table--edit-row">
        <RoleEditingRow
          role={role}
          onEdit={onEdit}
          onSave={onSave}
          isNew={isNew}
        />
        <td className="admin-table--left-offset">--</td>
        <td className="admin-table--left-offset">--</td>
        <td
          className="text-right"
          style={{width: `${ROLES_TABLE.colDelete}px`}}
        >
          <ConfirmOrCancel
            item={role}
            onConfirm={onSave}
            onCancel={onCancel}
            buttonSize="btn-xs"
          />
        </td>
      </tr>
    )
  }

  const wrappedDelete = () => {
    onDelete(role)
  }

  return (
    <tr>
      <td style={{width: `${ROLES_TABLE.colName}px`}}>{roleName}</td>
      <td>
        {allPermissions && allPermissions.length ? (
          <MultiSelectDropdown
            items={allPermissions.map(name => ({name}))}
            selectedItems={perms.map(name => ({name}))}
            label={perms.length ? '' : 'Select Permissions'}
            onApply={handleUpdatePermissions}
            buttonSize="btn-xs"
            buttonColor="btn-primary"
            customClass={classnames(`dropdown-${ROLES_TABLE.colPermissions}`, {
              'admin-table--multi-select-empty': !permissions.length,
            })}
            resetStateOnReceiveProps={false}
          />
        ) : null}
      </td>
      <td>
        {allUsers && allUsers.length ? (
          <MultiSelectDropdown
            items={allUsers}
            selectedItems={users}
            label={users.length ? '' : 'Select Users'}
            onApply={handleUpdateUsers}
            buttonSize="btn-xs"
            buttonColor="btn-primary"
            customClass={classnames(`dropdown-${ROLES_TABLE.colUsers}`, {
              'admin-table--multi-select-empty': !users.length,
            })}
            resetStateOnReceiveProps={false}
          />
        ) : null}
      </td>
      <td className="text-right">
        <ConfirmButton
          customClass="table--show-on-row-hover"
          size="btn-xs"
          type="btn-danger"
          text="Delete Role"
          confirmAction={wrappedDelete}
        />
      </td>
    </tr>
  )
}

const {arrayOf, bool, func, shape, string} = PropTypes

RoleRow.propTypes = {
  role: shape({
    name: string,
    permissions: arrayOf(
      shape({
        name: string,
      })
    ),
    users: arrayOf(
      shape({
        name: string,
      })
    ),
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
