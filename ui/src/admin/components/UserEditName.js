import React, {Component, PropTypes} from 'react'

import {USERS_TABLE} from 'src/admin/constants/tableSizing'

class UserEditName extends Component {
  constructor(props) {
    super(props)

    this.handleKeyPress = ::this.handleKeyPress
    this.handleEdit = ::this.handleEdit
  }

  handleKeyPress(user) {
    return e => {
      if (e.key === 'Enter') {
        this.props.onSave(user)
      }
    }
  }

  handleEdit(user) {
    return e => {
      this.props.onEdit(user, {[e.target.name]: e.target.value})
    }
  }

  render() {
    const {user} = this.props
    return (
      <td style={{width: `${USERS_TABLE.colUsername}px`}}>
        <input
          className="form-control input-xs"
          name="name"
          type="text"
          value={user.name || ''}
          placeholder="Username"
          onChange={this.handleEdit(user)}
          onKeyPress={this.handleKeyPress(user)}
          autoFocus={true}
          spellCheck={false}
          autoComplete={false}
        />
      </td>
    )
  }
}

const {func, shape} = PropTypes

UserEditName.propTypes = {
  user: shape().isRequired,
  onEdit: func.isRequired,
  onSave: func.isRequired,
}

export default UserEditName
