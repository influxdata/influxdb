import React, {PropTypes} from 'react'

import Dropdown from 'shared/components/Dropdown'

import {DEFAULT_ORG_NAME, NO_ORG, NO_ROLE} from 'src/admin/constants/dummyUsers'
import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'

const UsersTableOrgCell = ({
  user,
  organizations,
  onChangeUserRole,
  onChooseFilter,
}) => {
  const {colOrg} = USERS_TABLE

  // Expects Users to always have at least 1 role (as a member of the default org)
  if (user.roles.length === 1) {
    return (
      <td style={{width: colOrg}}>
        <Dropdown
          items={organizations
            .filter(org => {
              return !(org.name === DEFAULT_ORG_NAME || org.name === NO_ORG)
            })
            .map(r => ({
              ...r,
              text: r.name,
            }))}
          selected={NO_ORG}
          onChoose={onChangeUserRole(user, NO_ROLE)}
          buttonColor="btn-primary"
          buttonSize="btn-xs"
          className="dropdown-190"
        />
      </td>
    )
  }

  return (
    <td style={{width: colOrg}}>
      {user.roles
        .filter(role => {
          return !(role.organizationName === DEFAULT_ORG_NAME)
        })
        .map((role, i) =>
          <span key={i} className="chronograf-user--org">
            <a href="#" onClick={onChooseFilter(role.organizationName)}>
              {role.organizationName}
            </a>
          </span>
        )}
    </td>
  )
}

const {arrayOf, func, shape} = PropTypes

UsersTableOrgCell.propTypes = {
  user: shape(),
  organizations: arrayOf(shape()),
  onChangeUserRole: func.isRequired,
  onChooseFilter: func.isRequired,
}

export default UsersTableOrgCell
