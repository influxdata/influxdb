import React, {PropTypes} from 'react'

import Dropdown from 'shared/components/Dropdown'

import {USER_ROLES} from 'src/admin/constants/dummyUsers'

const BatchActionsBar = ({
  organizations,
  onDeleteUsers,
  onChangeRoles,
  numUsersSelected,
  onAddUserToOrg,
}) => {
  const rolesDropdownItems = USER_ROLES.map(role => ({
    ...role,
    text: role.name,
  }))

  return (
    <div className="chronograf-admin-table--batch">
      <p className="chronograf-admin-table--num-selected">
        {numUsersSelected} User{numUsersSelected === 1 ? ' ' : 's '}Selected
      </p>
      {numUsersSelected > 0
        ? <div className="chronograf-admin-table--batch-actions">
            <div className="btn btn-sm btn-danger" onClick={onDeleteUsers}>
              Delete
            </div>
            <Dropdown
              items={rolesDropdownItems}
              selected={'Set New Role'}
              onChoose={onChangeRoles}
              buttonColor="btn-primary"
              className="dropdown-140"
            />
            <Dropdown
              items={organizations.map(org => ({...org, text: org.name}))}
              selected={'Add to Org'}
              onChoose={onAddUserToOrg}
              buttonColor="btn-primary"
              className="dropdown-240"
            />
          </div>
        : null}
    </div>
  )
}

const {arrayOf, func, number, shape} = PropTypes

BatchActionsBar.propTypes = {
  organizations: arrayOf(shape()),
  onDeleteUsers: func.isRequired,
  onChangeRoles: func.isRequired,
  numUsersSelected: number.isRequired,
  onAddUserToOrg: func.isRequired,
}

export default BatchActionsBar
