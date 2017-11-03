import React, {PropTypes} from 'react'

import Dropdown from 'shared/components/Dropdown'

import {DEFAULT_ORG_ID, USER_ROLES} from 'src/admin/constants/dummyUsers'
import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'

const UsersTableRoleCell = ({user, onChangeUserRole}) => {
  const {colRole} = USERS_TABLE

  // User must be part of more than one organization to be able to be assigned a role
  if (user.roles.length === 1) {
    return (
      <td style={{width: colRole}}>
        <span className="chronograf-user--role">N/A</span>
      </td>
    )
  }

  return (
    <td style={{width: colRole}}>
      {user.roles
        .filter(role => {
          return !(role.organization === DEFAULT_ORG_ID)
        })
        .map((role, i) =>
          <Dropdown
            key={i}
            items={USER_ROLES.map(r => ({
              ...r,
              text: r.name,
            }))}
            selected={role.name}
            onChoose={onChangeUserRole(user, role)}
            buttonColor="btn-primary"
            buttonSize="btn-xs"
            className="dropdown-80"
          />
        )}
    </td>
  )
}

const {arrayOf, func, shape, string} = PropTypes

UsersTableRoleCell.propTypes = {
  user: shape({
    roles: arrayOf(
      shape({
        name: string.isRequired,
        organization: string.isRequired,
      })
    ),
  }),
  onChangeUserRole: func.isRequired,
}

export default UsersTableRoleCell
