import React, {Component, PropTypes} from 'react'

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
          {panelTitle} in <em>{organization.name}</em>
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
}

const {bool, func, shape, string, number} = PropTypes

UsersTableHeader.defaultProps = {
  numUsers: 0,
  organization: {
    name: '',
  },
  isCreatingUser: false,
}

UsersTableHeader.propTypes = {
  numUsers: number.isRequired,
  onClickCreateUser: func,
  isCreatingUser: bool.isRequired,
  organization: shape({
    name: string.isRequired,
  }),
}

export default UsersTableHeader
