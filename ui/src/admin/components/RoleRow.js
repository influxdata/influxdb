import React, {PropTypes} from 'react'
import _ from 'lodash'

import MultiSelectDropdown from 'shared/components/MultiSelectDropdown'
import DeleteRow from 'src/admin/components/DeleteRow'

const PERMISSIONS = [
  "NoPermissions",
  "ViewAdmin",
  "ViewChronograf",
  "CreateDatabase",
  "CreateUserAndRole",
  "AddRemoveNode",
  "DropDatabase",
  "DropData",
  "ReadData",
  "WriteData",
  "Rebalance",
  "ManageShard",
  "ManageContinuousQuery",
  "ManageQuery",
  "ManageSubscription",
  "Monitor",
  "CopyShard",
  "KapacitorAPI",
  "KapacitorConfigAPI",
]

const RoleRow = ({role: {name, permissions, users}, role, onDelete}) => (
  <tr>
    <td>{name}</td>
    <td>
      {
        permissions && permissions.length ?
        <MultiSelectDropdown
          items={PERMISSIONS}
          selectedItems={_.get(permissions, ['0', 'allowed'], [])}
          label={'Select Permissions'}
          onApply={() => '//TODO'}
        /> : '\u2014'
      }
    </td>
    <td>
      {
        users && users.length ?
        <MultiSelectDropdown
          items={users.map((r) => r.name)}
          selectedItems={[]}
          label={'Select Users'}
          onApply={() => '//TODO'}
        /> : '\u2014'
      }
    </td>
    <td className="text-right" style={{width: "85px"}}>
      <DeleteRow onDelete={onDelete} item={role} />
    </td>
  </tr>
)

const {
  arrayOf,
  func,
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
  onDelete: func.isRequired,
}

export default RoleRow
