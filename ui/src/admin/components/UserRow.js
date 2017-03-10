import React, {PropTypes} from 'react'

import EditingRow from 'src/admin/components/EditingRow'
import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'
import ConfirmButtons from 'src/admin/components/ConfirmButtons'
import DeleteRow from 'src/admin/components/DeleteRow'

const UserRow = ({
  user: {name, roles, permissions},
  user,
  allRoles,
  isNew,
  isEditing,
  onEdit,
  onSave,
  onCancel,
  onDelete,
}) => (
  <tr>
    {
      isEditing ?
        <EditingRow user={user} onEdit={onEdit} onSave={onSave} isNew={isNew} /> :
        <td>{name}</td>
    }
    <td>
      {
        allRoles && allRoles.length ?
          <MultiSelectDropdown
            items={allRoles.map((r) => r.name)}
            selectedItems={roles ? roles.map((r) => r.name) : []/* TODO remove check when server returns empty list */}
            label={roles.length ? '' : 'Select Roles'}
            onApply={() => '//TODO'}
          /> :
          '\u2014'
      }
    </td>
    <td>
      {
        permissions && permissions.length ?
          <MultiSelectDropdown
            items={[]}
            selectedItems={[]}
            label={'Select Permissions'}
            onApply={() => '//TODO'}
          /> :
          '\u2014'
      }
    </td>
    <td className="text-right" style={{width: "85px"}}>
      {isEditing ?
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
  isNew: bool,
  isEditing: bool,
  onCancel: func,
  onEdit: func,
  onSave: func,
  onDelete: func.isRequired,
}

export default UserRow
