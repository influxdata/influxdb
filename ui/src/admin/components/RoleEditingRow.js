import React, {Component, PropTypes} from 'react'

class RoleEditingRow extends Component {
  constructor(props) {
    super(props)

    this.handleKeyPress = ::this.handleKeyPress
    this.handleEdit = ::this.handleEdit
  }

  handleKeyPress(role) {
    return (e) => {
      if (e.key === 'Enter') {
        this.props.onSave(role)
      }
    }
  }

  handleEdit(role) {
    return (e) => {
      this.props.onEdit(role, {[e.target.name]: e.target.value})
    }
  }

  render() {
    const {role} = this.props
    return (
      <td>
        <input
          name="name"
          type="text"
          value={role.name || ''}
          placeholder="role name"
          onChange={this.handleEdit(role)}
          onKeyPress={this.handleKeyPress(role)}
          autoFocus={true}
        />
      </td>
    )
  }
}

const {
  bool,
  func,
  shape,
} = PropTypes

RoleEditingRow.propTypes = {
  role: shape().isRequired,
  isNew: bool,
  onEdit: func.isRequired,
  onSave: func.isRequired,
}

export default RoleEditingRow
