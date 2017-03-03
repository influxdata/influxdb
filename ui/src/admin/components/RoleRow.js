import React, {PropTypes} from 'react'
import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'

const RoleRow = ({role: {name, permissions, users}}) => (
  <tr>
    <td>{name}</td>
    <td>
      {
        permissions && permissions.length ?
        <MultiSelectDropdown
          items={permissions.map((perm) => perm.name)}
          selectedItems={[]}
          label={'Select Permissions'}
          onApply={() => '//TODO'}
        /> : '\u2014'
      }
    </td>
    <td>
      {
        users && users.length ?
        <MultiSelectDropdown
          items={users.map((role) => role.name)}
          selectedItems={[]}
          label={'Select Users'}
          onApply={() => '//TODO'}
        /> : '\u2014'
      }
    </td>
  </tr>
)

const {
  arrayOf,
  shape,
  string,
} = PropTypes

RoleRow.propTypes = {
  role: shape({
    name: string,
    permissions: arrayOf(shape({
      name: string,
    })),
    users: arrayOf(shape({
      name: string,
    })),
  }).isRequired,
}

export default RoleRow