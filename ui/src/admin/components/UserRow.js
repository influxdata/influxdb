import React, {PropTypes} from 'react'

import EditingRow from 'src/admin/components/EditingRow'
import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'
import ConfirmButtons from 'src/admin/components/ConfirmButtons'
import DeleteRow from 'src/admin/components/DeleteRow'
import classNames from 'classnames'

const UserRow = ({
  user: {name, roles, permissions},
  user,
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
        <EditingRow user={user} onEdit={onEdit} onSave={onSave} isNew={isNew} /> :
        <td>{name}</td>
    }
    <td>
      {
        roles && roles.length ?
          <MultiSelectDropdown
            items={roles.map((role) => role.name)}
            selectedItems={[]}
            label={'Select Roles'}
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
  isNew: bool,
  isEditing: bool,
  onCancel: func,
  onEdit: func,
  onSave: func,
  onDelete: func.isRequired,
}

export default UserRow
