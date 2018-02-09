import React, {PropTypes} from 'react'

import SlideToggle from 'shared/components/SlideToggle'

const AllUsersTableHeader = ({
  numUsers,
  numOrganizations,
  onClickCreateUser,
  isCreatingUser,
  authConfig: {superAdminNewUsers},
  onChangeAuthConfig,
}) => {
  const numUsersString = `${numUsers} User${numUsers === 1 ? '' : 's'}`
  const numOrganizationsString = `${numOrganizations} Org${numOrganizations ===
  1
    ? ''
    : 's'}`

  return (
    <div className="panel-heading u-flex u-ai-center u-jc-space-between">
      <h2 className="panel-title">
        {numUsersString} in {numOrganizationsString}
      </h2>
      <div style={{display: 'flex', alignItems: 'center'}}>
        <div className="all-users-admin-toggle">
          <SlideToggle
            size="xs"
            active={superAdminNewUsers}
            onToggle={onChangeAuthConfig('superAdminNewUsers')}
          />
          <span>All new users are SuperAdmins</span>
        </div>
        <button
          className="btn btn-primary btn-sm"
          onClick={onClickCreateUser}
          disabled={isCreatingUser || !onClickCreateUser}
        >
          <span className="icon plus" />
          Add User
        </button>
      </div>
    </div>
  )
}

const {bool, func, number, shape} = PropTypes

AllUsersTableHeader.defaultProps = {
  numUsers: 0,
  numOrganizations: 0,
  isCreatingUser: false,
}

AllUsersTableHeader.propTypes = {
  numUsers: number.isRequired,
  numOrganizations: number.isRequired,
  onClickCreateUser: func,
  isCreatingUser: bool.isRequired,
  onChangeAuthConfig: func.isRequired,
  authConfig: shape({
    superAdminNewUsers: bool,
  }),
}

export default AllUsersTableHeader
