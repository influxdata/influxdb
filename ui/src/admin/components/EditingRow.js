import React, {Component, PropTypes} from 'react'

class EditingRow extends Component {
  constructor(props) {
    super(props)

    this.handleKeyPress = ::this.handleKeyPress
    this.handleEdit = ::this.handleEdit
  }

  handleKeyPress(user) {
    return (e) => {
      if (e.key === 'Enter') {
        this.props.onSave(user)
      }
    }
  }

  handleEdit(user) {
    return (e) => {
      this.props.onEdit(user, {[e.target.name]: e.target.value})
    }
  }

  render() {
    const {user, isNew} = this.props
    return (
      <td>
        <input
          name="name"
          type="text"
          value={user.name || ''}
          placeholder="username"
          onChange={this.handleEdit(user)}
          onKeyPress={this.handleKeyPress(user)}
          autoFocus={true}
        />
        {
          isNew ?
            <input
              name="password"
              type="text"
              value={user.password || ''}
              placeholder="password"
              onChange={this.handleEdit(user)}
              onKeyPress={this.handleKeyPress(user)}
            /> :
            null
        }
      </td>
    )
  }
}

const {
  bool,
  func,
  shape,
} = PropTypes

EditingRow.propTypes = {
  user: shape().isRequired,
  isNew: bool,
  onEdit: func.isRequired,
  onSave: func.isRequired,
}

export default EditingRow