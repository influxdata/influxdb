import React from 'react'
import PropTypes from 'prop-types'

import Dropdown from 'shared/components/Dropdown'
import ConfirmButton from 'shared/components/ConfirmButton'

import {USER_ROLES} from 'src/admin/constants/chronografAdmin'
import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'

const UsersTableRow = ({
  user,
  organization,
  onChangeUserRole,
  onDelete,
  meID,
}) => {
  const {colRole, colProvider, colScheme} = USERS_TABLE

  const dropdownRolesItems = USER_ROLES.map(r => ({
    ...r,
    text: r.name,
  }))
  const currentRole = user.roles.find(
    role => role.organization === organization.id
  )

  const userIsMe = user.id === meID

  const wrappedDelete = () => {
    onDelete(user)
  }

  const removeWarning = 'Remove this user\nfrom Current Org?'

  return (
    <tr className={'chronograf-admin-table--user'}>
      <td>
        {userIsMe ? (
          <strong className="chronograf-user--me">
            <span className="icon user" />
            {user.name}
          </strong>
        ) : (
          <strong>{user.name}</strong>
        )}
      </td>
      <td style={{width: colRole}}>
        <span className="chronograf-user--role">
          <Dropdown
            items={dropdownRolesItems}
            selected={currentRole.name}
            onChoose={onChangeUserRole(user, currentRole)}
            buttonColor="btn-primary"
            buttonSize="btn-xs"
            className="dropdown-stretch"
          />
        </span>
      </td>
      <td style={{width: colProvider}}>{user.provider}</td>
      <td style={{width: colScheme}}>{user.scheme}</td>
      <td className="text-right">
        <ConfirmButton
          confirmText={removeWarning}
          confirmAction={wrappedDelete}
          size="btn-xs"
          type="btn-danger"
          text="Remove"
          customClass="table--show-on-row-hover"
        />
      </td>
    </tr>
  )
}

const {func, shape, string} = PropTypes

UsersTableRow.propTypes = {
  user: shape(),
  organization: shape({
    name: string.isRequired,
    id: string.isRequired,
  }),
  onChangeUserRole: func.isRequired,
  onDelete: func.isRequired,
  meID: string.isRequired,
}

export default UsersTableRow
