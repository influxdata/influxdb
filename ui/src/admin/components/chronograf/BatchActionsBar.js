import React, {PropTypes} from 'react'

import Dropdown from 'shared/components/Dropdown'

import {
  DEFAULT_ORG_NAME,
  NO_ORG,
  USER_ROLES,
} from 'src/admin/constants/dummyUsers'

const BatchActionsBar = ({
  onDeleteUsers,
  onAddUsersToOrg,
  onRemoveUsersFromOrg,
  onChangeRoles,
  numUsersSelected,
  organizationName,
  organizations,
}) => {
  const showWhenAllUsers = organizationName === DEFAULT_ORG_NAME
  const showWhenNotAllUser = organizationName !== DEFAULT_ORG_NAME

  const sanitizedOrgs = organizations
    .filter(org => {
      return !(org.name === DEFAULT_ORG_NAME || org.name === NO_ORG)
    })
    .map(org => ({
      ...org,
      text: org.name,
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
            {showWhenAllUsers
              ? <Dropdown
                  items={sanitizedOrgs}
                  selected={'Add to Organization'}
                  onChoose={onAddUsersToOrg}
                  buttonColor="btn-primary"
                  className="dropdown-200"
                />
              : null}
            {showWhenAllUsers
              ? <Dropdown
                  items={sanitizedOrgs}
                  selected={'Remove from Organization'}
                  onChoose={onRemoveUsersFromOrg}
                  buttonColor="btn-primary"
                  className="dropdown-200"
                />
              : null}
            {showWhenNotAllUser
              ? <Dropdown
                  items={USER_ROLES.map(role => ({
                    ...role,
                    text: role.name,
                  }))}
                  selected={'Change Role'}
                  onChoose={onChangeRoles}
                  buttonColor="btn-primary"
                  className="dropdown-140"
                />
              : null}
          </div>
        : null}
    </div>
  )
}

const {arrayOf, func, number, shape, string} = PropTypes

BatchActionsBar.propTypes = {
  onDeleteUsers: func.isRequired,
  onAddUsersToOrg: func.isRequired,
  onRemoveUsersFromOrg: func.isRequired,
  onChangeRoles: func.isRequired,
  organizations: arrayOf(shape),
  numUsersSelected: number.isRequired,
  organizationName: string.isRequired,
}

export default BatchActionsBar
