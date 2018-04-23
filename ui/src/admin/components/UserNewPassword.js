import React, {Component} from 'react'
import PropTypes from 'prop-types'

import {USERS_TABLE} from 'src/admin/constants/tableSizing'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class UserNewPassword extends Component {
  handleKeyPress = user => {
    return e => {
      if (e.key === 'Enter') {
        this.props.onSave(user)
      }
    }
  }

  handleEdit = user => {
    return e => {
      this.props.onEdit(user, {[e.target.name]: e.target.value})
    }
  }

  render() {
    const {user, isNew} = this.props
    return (
      <td style={{width: `${USERS_TABLE.colPassword}px`}}>
        {isNew ? (
          <input
            className="form-control input-xs"
            name="password"
            type="password"
            value={user.password || ''}
            placeholder="Password"
            onChange={this.handleEdit(user)}
            onKeyPress={this.handleKeyPress(user)}
            spellCheck={false}
            autoComplete="false"
          />
        ) : (
          '--'
        )}
      </td>
    )
  }
}

const {bool, func, shape} = PropTypes

UserNewPassword.propTypes = {
  user: shape().isRequired,
  isNew: bool,
  onEdit: func.isRequired,
  onSave: func.isRequired,
}

export default UserNewPassword
