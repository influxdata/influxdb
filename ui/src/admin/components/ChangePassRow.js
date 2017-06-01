import React, {Component, PropTypes} from 'react'

import OnClickOutside from 'shared/components/OnClickOutside'
import ConfirmButtons from 'shared/components/ConfirmButtons'

class ChangePassRow extends Component {
  constructor(props) {
    super(props)
    this.state = {
      showForm: false,
    }
    this.showForm = ::this.showForm
    this.handleCancel = ::this.handleCancel
    this.handleKeyPress = ::this.handleKeyPress
    this.handleEdit = ::this.handleEdit
    this.handleSubmit = ::this.handleSubmit
  }

  showForm() {
    this.setState({showForm: true})
  }

  handleCancel() {
    this.setState({showForm: false})
  }

  handleClickOutside() {
    this.setState({showForm: false})
  }

  handleSubmit(user) {
    this.props.onApply(user)
    this.setState({showForm: false})
  }

  handleKeyPress(user) {
    return e => {
      if (e.key === 'Enter') {
        this.handleSubmit(user)
      }
    }
  }

  handleEdit(user) {
    return e => {
      this.props.onEdit(user, {[e.target.name]: e.target.value})
    }
  }

  render() {
    const {user, buttonSize} = this.props

    if (this.state.showForm) {
      return (
        <div className="admin-table--change-pw">
          <input
            className="form-control input-xs"
            name="password"
            type="password"
            value={user.password || ''}
            placeholder="New password"
            onChange={this.handleEdit(user)}
            onKeyPress={this.handleKeyPress(user)}
            autoFocus={true}
          />
          <ConfirmButtons
            onConfirm={this.handleSubmit}
            item={user}
            onCancel={this.handleCancel}
            buttonSize={buttonSize}
          />
        </div>
      )
    }

    return (
      <div className="admin-table--change-pw">
        <a href="#" onClick={this.showForm}>
          Change
        </a>
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

ChangePassRow.propTypes = {
  user: shape().isRequired,
  onApply: func.isRequired,
  onEdit: func.isRequired,
  buttonSize: string,
}

export default OnClickOutside(ChangePassRow)
