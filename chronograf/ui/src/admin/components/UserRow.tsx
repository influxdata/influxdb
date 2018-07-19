import React, {PureComponent} from 'react'

import UserPermissionsDropdown from 'src/admin/components/UserPermissionsDropdown'
import UserRoleDropdown from 'src/admin/components/UserRoleDropdown'
import ChangePassRow from 'src/admin/components/ChangePassRow'
import ConfirmButton from 'src/shared/components/ConfirmButton'
import {USERS_TABLE} from 'src/admin/constants/tableSizing'

import UserRowEdit from 'src/admin/components/UserRowEdit'
import {User} from 'src/types/influxAdmin'
import {ErrorHandling} from 'src/shared/decorators/errors'

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

@ErrorHandling
class UserRow extends PureComponent<UserRowProps> {
  public render() {
    const {
      user,
      allRoles,
      allPermissions,
      hasRoles,
      isNew,
      isEditing,
      onEdit,
      onSave,
      onCancel,
      onUpdatePermissions,
      onUpdateRoles,
    } = this.props

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
        <td style={{width: `${USERS_TABLE.colUsername}px`}}>{user.name}</td>
        <td style={{width: `${USERS_TABLE.colPassword}px`}}>
          <ChangePassRow
            user={user}
            onEdit={onEdit}
            buttonSize="btn-xs"
            onApply={this.handleUpdatePassword}
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
          {this.hasPermissions && (
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
            confirmAction={this.handleDelete}
            customClass="table--show-on-row-hover"
          />
        </td>
      </tr>
    )
  }

  private handleDelete = (): void => {
    const {user, onDelete} = this.props

    onDelete(user)
  }

  private handleUpdatePassword = (): void => {
    const {user, onUpdatePassword} = this.props

    onUpdatePassword(user, user.password)
  }

  private get hasPermissions() {
    const {allPermissions} = this.props
    return allPermissions && !!allPermissions.length
  }
}

export default UserRow
