import React, {PropTypes} from 'react'

import Dropdown from 'shared/components/Dropdown'

import {
  DEFAULT_ORG_NAME,
  DEFAULT_ORG_ID,
  NO_ORG,
} from 'src/admin/constants/dummyUsers'
import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'

const UsersTableOrgCell = ({
  user,
  organizations,
  onSelectAddUserToOrg,
  onChooseFilter,
}) => {
  const {colOrg} = USERS_TABLE

  // Expects Users to always have at least 1 role (as a member of the default org)
  if (user.roles.length === 1) {
    return (
      <td style={{width: colOrg}}>
        <Dropdown
          items={organizations
            .filter(
              org => !(org.name === DEFAULT_ORG_NAME || org.name === NO_ORG)
            )
            .map(r => ({
              ...r,
              text: r.name,
            }))}
          selected={'Add to organization'}
          // TODO: assigning role here may not be necessary especially once
          // default organization roles are implemented
          onChoose={onSelectAddUserToOrg}
          buttonColor="btn-primary"
          buttonSize="btn-xs"
          className="dropdown-190"
        />
      </td>
    )
  }

  return organizations.length
    ? <td style={{width: colOrg}}>
        {user.roles
          .filter(role => {
            return !(role.organization === DEFAULT_ORG_ID)
          })
          .map((role, i) => {
            const org = organizations.find(o => o.id === role.organization)
            return (
              <span key={i} className="chronograf-user--org">
                <a href="#" onClick={onChooseFilter(org)}>
                  {org.name}
                </a>
              </span>
            )
          })}
      </td>
    : null
}

const {arrayOf, func, shape, string} = PropTypes

UsersTableOrgCell.propTypes = {
  user: shape({
    roles: arrayOf(
      shape({
        name: string.isRequired,
        organization: string.isRequired,
      })
    ),
  }),
  organizations: arrayOf(shape()),
  onSelectAddUserToOrg: func.isRequired,
  onChooseFilter: func.isRequired,
}

export default UsersTableOrgCell
