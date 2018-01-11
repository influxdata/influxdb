import React, {PropTypes} from 'react'

import Dropdown from 'shared/components/Dropdown'
import SlideToggle from 'shared/components/SlideToggle'
import DeleteConfirmTableCell from 'shared/components/DeleteConfirmTableCell'

import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'
const {
  colOrganizations,
  colProvider,
  colScheme,
  colSuperAdmin,
  colRole,
} = USERS_TABLE

const AllUsersTableRow = ({
  organizations,
  user,
  onAddUserToOrganization,
  onChangeSuperAdmin,
  onDelete,
  meID,
}) => {
  const dropdownOrganizationsItems = organizations.map(r => ({
    ...r,
    text: r.name,
  }))

  const userIsMe = user.id === meID

  const userOrganizations = user.roles.map(r => ({
    ...r,
    name: organizations.find(o => r.organization === o.id).name,
  }))

  return (
    <tr className={'chronograf-admin-table--user'}>
      <td>
        {userIsMe
          ? <strong className="chronograf-user--me">
              <span className="icon user" />
              {user.name}
            </strong>
          : <strong>
              {user.name}
            </strong>}
      </td>
      <td style={{width: colOrganizations}}>
        {userOrganizations.map(o => o.name).join(', ')}
      </td>
      <td style={{width: colProvider}}>
        {user.provider}
      </td>
      <td style={{width: colScheme}}>
        {user.scheme}
      </td>
      <td style={{width: colSuperAdmin}} className="text-center">
        <SlideToggle
          active={user.superAdmin}
          onToggle={onChangeSuperAdmin(user)}
          size="xs"
          disabled={userIsMe}
        />
      </td>
      <td style={{width: colRole}}>
        <span className="chronograf-user--role">
          <Dropdown
            items={dropdownOrganizationsItems}
            selected={'Add to Organization'}
            onChoose={onAddUserToOrganization(user)}
            buttonColor="btn-primary"
            buttonSize="btn-xs"
            className="dropdown-stretch"
          />
        </span>
      </td>
      <DeleteConfirmTableCell
        text="Remove"
        onDelete={onDelete}
        item={user}
        buttonSize="btn-xs"
        disabled={userIsMe}
      />
    </tr>
  )
}

const {arrayOf, func, shape, string} = PropTypes

AllUsersTableRow.propTypes = {
  user: shape(),
  organization: shape({
    name: string.isRequired,
    id: string.isRequired,
  }),
  onAddUserToOrganization: func.isRequired,
  onChangeSuperAdmin: func.isRequired,
  onDelete: func.isRequired,
  meID: string.isRequired,
  organizations: arrayOf(
    shape({
      id: string.isRequired,
      name: string.isRequired,
    })
  ),
}

export default AllUsersTableRow
