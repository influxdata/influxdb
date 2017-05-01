import React, {Component, PropTypes} from 'react'

class UserEditingRow extends Component {
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
    const {user, isNew} = this.props
    return (
      <td>
        <div className="admin-table--edit-cell">
          <input
            className="form-control"
            name="name"
            type="text"
            value={user.name || ''}
            placeholder="Username"
            onChange={this.handleEdit(user)}
            onKeyPress={this.handleKeyPress(user)}
            autoFocus={true}
          />
          {isNew
            ? <input
                className="form-control"
                name="password"
                type="password"
                value={user.password || ''}
                placeholder="Password"
                onChange={this.handleEdit(user)}
                onKeyPress={this.handleKeyPress(user)}
              />
            : null}
        </div>
      </td>
    )
  }
}

const {bool, func, shape} = PropTypes

UserEditingRow.propTypes = {
  user: shape().isRequired,
  isNew: bool,
  onEdit: func.isRequired,
  onSave: func.isRequired,
}

export default UserEditingRow
