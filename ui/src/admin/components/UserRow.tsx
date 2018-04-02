import React, {PureComponent} from 'react'

import UserPermissionsDropdown from 'src/admin/components/UserPermissionsDropdown'
import UserRoleDropdown from 'src/admin/components/UserRoleDropdown'
import ChangePassRow from 'src/admin/components/ChangePassRow'
import ConfirmButton from 'src/shared/components/ConfirmButton'
import {USERS_TABLE} from 'src/admin/constants/tableSizing'

import UserRowEdit from 'src/admin/components/UserRowEdit'
import {User} from 'src/types/influxAdmin'

interface UserRowProps {
  user: User
  allRoles: any[]
  allPermissions: string[]
  hasRoles: boolean
  isNew: boolean
  isEditing: boolean
  onCancel: () => void
  onEdit: () => void
  onSave: () => void
  onDelete: (user: User) => void
  onUpdatePermissions: (user: User, permissions: any[]) => void
  onUpdateRoles: (user: User, roles: any[]) => void
  onUpdatePassword: (user: User, password: string) => void
}

class UserRow extends PureComponent<UserRowProps> {
  public render() {
    const {
      user: {name, password},
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
    } = this.props

    function handleUpdatePassword(): void {
      onUpdatePassword(user, password)
    }

    const wrappedDelete = () => {
      onDelete(user)
    }

    if (isEditing) {
      return (
        <UserRowEdit
          user={user}
          isNew={isNew}
          onEdit={onEdit}
          onSave={onSave}
          onCancel={onCancel}
          hasRoles={hasRoles}
        />
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
        {hasRoles && (
          <td>
            <UserRoleDropdown
              user={user}
              allRoles={allRoles}
              onUpdateRoles={onUpdateRoles}
            />
          </td>
        )}
        <td>
          {allPermissions &&
            !!allPermissions.length && (
              <UserPermissionsDropdown
                user={user}
                allPermissions={allPermissions}
                onUpdatePermissions={onUpdatePermissions}
              />
            )}
        </td>
        <td
          className="text-right"
          style={{width: `${USERS_TABLE.colDelete}px`}}
        >
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
}

export default UserRow
