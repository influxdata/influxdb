import React, {PropTypes} from 'react'

import Dropdown from 'shared/components/Dropdown'

import {DEFAULT_ORG, USER_ROLES} from 'src/admin/constants/dummyUsers'

const UsersTableRoleCell = ({user, organizationName, onChangeUserRole}) => {
  // User must be part of more than one organization to be able to be assigned a role
  if (user.roles.length === 1) {
    return <span className="chronograf-user--role">N/A</span>
  }

  if (organizationName === DEFAULT_ORG) {
    return user.roles
      .filter(role => {
        return !(role.organizationName === DEFAULT_ORG)
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
      )
  }

  const currentRole = user.roles.find(
    role => role.organizationName === organizationName
  )
  return (
    <span className="chronograf-user--role">
      <Dropdown
        items={USER_ROLES.map(r => ({
          ...r,
          text: r.name,
        }))}
        selected={currentRole.name}
        onChoose={onChangeUserRole(user, currentRole)}
        buttonColor="btn-primary"
        buttonSize="btn-xs"
        className="dropdown-80"
      />
    </span>
  )
}

const {func, shape, string} = PropTypes

UsersTableRoleCell.propTypes = {
  user: shape(),
  organizationName: string.isRequired,
  onChangeUserRole: func.isRequired,
}

export default UsersTableRoleCell
