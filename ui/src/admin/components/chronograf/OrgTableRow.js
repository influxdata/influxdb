import React, {PropTypes} from 'react'

import Authorized, {SUPERADMIN_ROLE} from 'src/auth/Authorized'

import Dropdown from 'shared/components/Dropdown'

import {
  USER_ROLES,
  DEFAULT_ORG_NAME,
  NO_ORG,
  MEMBER_ROLE,
  DUMMY_ORGS,
} from 'src/admin/constants/dummyUsers'
import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'

const OrgTableRow = ({
  user,
  organizationName,
  onToggleUserSelected,
  selectedUsers,
  isSameUser,
  onChangeUserRole,
}) => {
  const {colOrg, colRole, colSuperAdmin, colProvider, colScheme} = USERS_TABLE

  const isSelected = selectedUsers.find(u => isSameUser(user, u))

  const currentRole = user.roles.find(
    role => role.organizationName === organizationName
  )

  return (
    <tr className={isSelected ? 'selected' : null}>
      <td
        onClick={onToggleUserSelected(user)}
        className="chronograf-admin-table--check-col chronograf-admin-table--selectable"
      >
        <div className="user-checkbox" />
      </td>
      <td
        onClick={onToggleUserSelected(user)}
        className="chronograf-admin-table--selectable"
      >
        <strong>
          {user.name}
        </strong>
      </td>
      <td style={{width: colOrg}}>
        {currentRole
          ? <span className="chronograf-user--org">
              {organizationName}
            </span>
          : <Dropdown
              items={DUMMY_ORGS.filter(org => {
                return !(org.name === DEFAULT_ORG_NAME || org.name === NO_ORG)
              }).map(r => ({
                ...r,
                text: r.name,
              }))}
              selected={NO_ORG}
              onChoose={onChangeUserRole(user, MEMBER_ROLE)}
              buttonColor="btn-primary"
              buttonSize="btn-xs"
              className="dropdown-190"
            />}
      </td>
      <td style={{width: colRole}}>
        <span className="chronograf-user--role">
          {currentRole
            ? <Dropdown
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
            : 'N/A'}
        </span>
      </td>
      <Authorized requiredRole={SUPERADMIN_ROLE}>
        <td style={{width: colSuperAdmin}}>
          {user.superadmin ? 'Yes' : '--'}
        </td>
      </Authorized>
      <td style={{width: colProvider}}>
        {user.provider}
      </td>
      <td className="text-right" style={{width: colScheme}}>
        {user.scheme}
      </td>
    </tr>
  )
}

const {arrayOf, func, shape, string} = PropTypes

OrgTableRow.propTypes = {
  user: shape(),
  organizationName: string.isRequired,
  onToggleUserSelected: func.isRequired,
  selectedUsers: arrayOf(shape()),
  isSameUser: func.isRequired,
  onChangeUserRole: func.isRequired,
}

export default OrgTableRow
