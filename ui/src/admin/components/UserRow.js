import React from 'react'
import PropTypes from 'prop-types'

import _ from 'lodash'
import classnames from 'classnames'

import UserEditName from 'src/admin/components/UserEditName'
import UserNewPassword from 'src/admin/components/UserNewPassword'
import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'
import ConfirmOrCancel from 'shared/components/ConfirmOrCancel'
import ConfirmButton from 'shared/components/ConfirmButton'
import ChangePassRow from 'src/admin/components/ChangePassRow'
import {USERS_TABLE} from 'src/admin/constants/tableSizing'

const UserRow = ({
  user: {name, roles = [], permissions, password},
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
  function handleUpdatePermissions(perms) {
    const allowed = perms.map(p => p.name)
    onUpdatePermissions(user, [{scope: 'all', allowed}])
  }

  function handleUpdateRoles(roleNames) {
    onUpdateRoles(
      user,
      allRoles.filter(r => roleNames.find(rn => rn.name === r.name))
    )
  }

  function handleUpdatePassword() {
    onUpdatePassword(user, password)
  }

  const perms = _.get(permissions, ['0', 'allowed'], [])

  const wrappedDelete = () => {
    onDelete(user)
  }

  if (isEditing) {
    return (
      <tr className="admin-table--edit-row">
        <UserEditName user={user} onEdit={onEdit} onSave={onSave} />
        <UserNewPassword
          user={user}
          onEdit={onEdit}
          onSave={onSave}
          isNew={isNew}
        />
        {hasRoles ? <td className="admin-table--left-offset">--</td> : null}
        <td className="admin-table--left-offset">--</td>
        <td
          className="text-right"
          style={{width: `${USERS_TABLE.colDelete}px`}}
        >
          <ConfirmOrCancel
            item={user}
            onConfirm={onSave}
            onCancel={onCancel}
            buttonSize="btn-xs"
          />
        </td>
      </tr>
    )
  }

  return (
    <tr>
      <td style={{width: `${USERS_TABLE.colUsername}px`}}>{name}</td>
      <td style={{width: `${USERS_TABLE.colPassword}px`}}>
        <ChangePassRow
          onEdit={onEdit}
          onApply={handleUpdatePassword}
          user={user}
          buttonSize="btn-xs"
        />
      </td>
      {hasRoles ? (
        <td>
          <MultiSelectDropdown
            items={allRoles}
            selectedItems={roles.map(r => ({name: r.name}))}
            label={roles.length ? '' : 'Select Roles'}
            onApply={handleUpdateRoles}
            buttonSize="btn-xs"
            buttonColor="btn-primary"
            customClass={classnames(`dropdown-${USERS_TABLE.colRoles}`, {
              'admin-table--multi-select-empty': !roles.length,
            })}
            resetStateOnReceiveProps={false}
          />
        </td>
      ) : null}
      <td>
        {allPermissions && allPermissions.length ? (
          <MultiSelectDropdown
            items={allPermissions.map(p => ({name: p}))}
            selectedItems={perms.map(p => ({name: p}))}
            label={
              permissions && permissions.length ? '' : 'Select Permissions'
            }
            onApply={handleUpdatePermissions}
            buttonSize="btn-xs"
            buttonColor="btn-primary"
            customClass={classnames(`dropdown-${USERS_TABLE.colPermissions}`, {
              'admin-table--multi-select-empty': !permissions.length,
            })}
            resetStateOnReceiveProps={false}
          />
        ) : null}
      </td>
      <td className="text-right" style={{width: `${USERS_TABLE.colDelete}px`}}>
        <ConfirmButton
          size="btn-xs"
          type="btn-danger"
          text="Delete User"
          confirmAction={wrappedDelete}
          customClass="table--show-on-row-hover"
        />
      </td>
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
