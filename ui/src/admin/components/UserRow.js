import React, {PropTypes} from 'react'
import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'
import DeleteRow from 'src/admin/components/DeleteRow'

const UserRow = ({
  user: {name, roles, permissions},
  user,
  isEditing,
  onSave,
  onInputChange,
  onInputKeyPress,
  onDelete,
}) => (
  <tr>
    {
      isEditing ?
        <td>
          <input
            name="name"
            type="text"
            placeholder="username"
            onChange={onInputChange}
            onKeyPress={onInputKeyPress}
            autoFocus={true}
          />
          <input
            name="password"
            type="text"
            placeholder="password"
            onChange={onInputChange}
            onKeyPress={onInputKeyPress}
          />
        </td>
        : <td>{name}</td>
    }
    <td>
      {
        roles && roles.length ?
        <MultiSelectDropdown
          items={roles.map((role) => role.name)}
          selectedItems={[]}
          label={'Select Roles'}
          onApply={() => '//TODO'}
        /> : '\u2014'
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
        /> : '\u2014'
      }
    </td>
    <td>
      {
        isEditing ?
        <div>
          <button onClick={onSave}>Save</button>
        </div>
        : null
      }
    </td>
    <td className="text-right" style={{width: "85px"}}>
      <DeleteRow onDelete={onDelete} item={user} />
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
  isEditing: bool,
  onSave: func,
  onInputChange: func,
  onInputKeyPress: func,
  onDelete: func.isRequired,
}

export default UserRow
