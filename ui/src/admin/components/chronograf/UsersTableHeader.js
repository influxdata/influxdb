import React, {Component, PropTypes} from 'react'
import Authorized, {ADMIN_ROLE} from 'src/auth/Authorized'

class UsersTableHeader extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {
      onClickCreateUser,
      numUsers,
      isCreatingUser,
      organization,
    } = this.props

    const panelTitle = numUsers === 1 ? `${numUsers} User` : `${numUsers} Users`

    return (
      <div className="panel-heading u-flex u-ai-center u-jc-space-between">
        <h2 className="panel-title">
          {panelTitle} in {organization.name}
        </h2>
        <Authorized requiredRole={ADMIN_ROLE}>
          <button
            className="btn btn-primary btn-sm"
            onClick={onClickCreateUser}
            disabled={isCreatingUser}
          >
            <span className="icon plus" />
            Create User
          </button>
        </Authorized>
      </div>
    )
  }
}

const {bool, func, shape, string, number} = PropTypes

UsersTableHeader.propTypes = {
  numUsers: number.isRequired,
  onClickCreateUser: func.isRequired,
  isCreatingUser: bool.isRequired,
  organization: shape({
    name: string.isRequired,
    id: string.isRequired,
  }),
}

export default UsersTableHeader
