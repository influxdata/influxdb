import React, {PropTypes} from 'react'
import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'

const UserRow = ({user: {name, roles, permissions}, isEditing, onCancel, onSave, onInputChange}) => (
  <tr>
    {
      isEditing ?
        <td>
          <input
            name="name"
            type="text"
            placeholder="username"
            onChange={onInputChange}
            autoFocus={true}
          />
          <input
            name="password"
            type="text"
            placeholder="password"
            onChange={onInputChange}
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
          <button onClick={onCancel}>Cancel</button>
          <button onClick={onSave}>Save</button>
        </div>
        : null
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
  isEditing: bool,
  onCancel: func,
  onSave: func,
  onInputChange: func,
}

export default UserRow
