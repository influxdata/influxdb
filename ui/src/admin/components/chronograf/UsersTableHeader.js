import React, {Component, PropTypes} from 'react'
import Authorized, {ADMIN_ROLE} from 'src/auth/Authorized'

class UsersTableHeader extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {onCreateUserRow, numUsers} = this.props

    const panelTitle = numUsers === 1 ? `${numUsers} User` : `${numUsers} Users`

    return (
      <div className="panel-heading u-flex u-ai-center u-jc-space-between">
        <h2 className="panel-title">
          {panelTitle}
        </h2>
        <Authorized requiredRole={ADMIN_ROLE}>
          <button className="btn btn-primary btn-sm" onClick={onCreateUserRow}>
            <span className="icon plus" />
            Create User
          </button>
        </Authorized>
      </div>
    )
  }
}

const {func, number} = PropTypes

UsersTableHeader.propTypes = {
  numUsers: number.isRequired,
  onCreateUserRow: func.isRequired,
}

export default UsersTableHeader
