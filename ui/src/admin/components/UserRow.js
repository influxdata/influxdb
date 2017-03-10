import React, {PropTypes} from 'react'
import _ from 'lodash'

import UserEditingRow from 'src/admin/components/UserEditingRow'
import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'
import ConfirmButtons from 'src/admin/components/ConfirmButtons'
import DeleteRow from 'src/admin/components/DeleteRow'
import classNames from 'classnames'

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
}) => (
  <tr className={classNames("", {"admin-table--edit-row": isEditing})}>
    {
      isEditing ?
        <UserEditingRow user={user} onEdit={onEdit} onSave={onSave} isNew={isNew} /> :
        <td>{name}</td>
    }
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
          /> :
          '\u2014'
      }
    </td>
    <td className="text-right" style={{width: "85px"}}>
      {
        isEditing ?
          <ConfirmButtons item={user} onConfirm={onSave} onCancel={onCancel} /> :
          <DeleteRow onDelete={onDelete} item={user} />
      }
    </td>
  </tr>
)

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
