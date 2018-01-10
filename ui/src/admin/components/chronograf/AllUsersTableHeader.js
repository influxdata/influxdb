import React, {PropTypes} from 'react'

const AllUsersTableHeader = ({
  numUsers,
  numOrganizations,
  onClickCreateUser,
  isCreatingUser,
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
      <button
        className="btn btn-primary btn-sm"
        onClick={onClickCreateUser}
        disabled={isCreatingUser || !onClickCreateUser}
      >
        <span className="icon plus" />
        Create User
      </button>
    </div>
  )
}

const {bool, func, number} = PropTypes

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
}

export default AllUsersTableHeader
