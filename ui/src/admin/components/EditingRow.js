import React, {Component, PropTypes} from 'react'

class EditingRow extends Component {
  constructor(props) {
    super(props)

    this.handleKeyPress = ::this.handleKeyPress
    this.handleEdit = ::this.handleEdit
    this.getValues = ::this.getValues
  }

  handleKeyPress(e) {
    if (e.key === 'Enter') {
      this.props.onSave()
    }
  }

  handleEdit() {
    this.props.onEdit(Object.assign(this.props.user, this.getValues()))
  }

  getValues() {
    const values = {name: this.refs.name.value}
    if (this.refs.hasOwnProperty('password')) {
      values.password = this.refs.password.value
    }
    return values
  }

  render() {
    const {isNew} = this.props
    return (
      <td>
        <input
          name="name"
          type="text"
          ref="name"
          placeholder="username"
          onKeyPress={this.handleKeyPress}
          onChange={this.handleEdit}
          autoFocus={true}
        />
        {
          isNew ?
            <input
              name="password"
              type="text"
              ref="password"
              placeholder="password"
              onChange={this.handleEdit}
              onKeyPress={this.handleKeyPress}
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