import React, {PropTypes} from 'react'
import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'
import DeleteRow from 'src/admin/components/DeleteRow'

const UserRow = ({user: {name, roles, permissions}, user, onDelete}) => (
  <tr>
    <td>{name}</td>
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
    <td className="text-right" style={{width: "85px"}}>
      <DeleteRow onDelete={onDelete} item={user} />
    </td>
  </tr>
)

const {
  arrayOf,
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
  onDelete: func.isRequired,
}

export default UserRow
