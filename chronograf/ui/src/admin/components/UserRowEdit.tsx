import React, {SFC} from 'react'
import UserEditName from 'src/admin/components/UserEditName'
import UserNewPassword from 'src/admin/components/UserNewPassword'
import ConfirmOrCancel from 'src/shared/components/ConfirmOrCancel'
import {USERS_TABLE} from 'src/admin/constants/tableSizing'

import {User} from 'src/types/influxAdmin'

interface UserRowEditProps {
  user: User
  onEdit: () => void
  onSave: () => void
  onCancel: () => void
  isNew: boolean
  hasRoles: boolean
}

const UserRowEdit: SFC<UserRowEditProps> = ({
  user,
  onEdit,
  onSave,
  onCancel,
  isNew,
  hasRoles,
}) => (
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
    <td className="text-right" style={{width: `${USERS_TABLE.colDelete}px`}}>
      <ConfirmOrCancel
        item={user}
        onConfirm={onSave}
        onCancel={onCancel}
        buttonSize="btn-xs"
      />
    </td>
  </tr>
)

export default UserRowEdit
