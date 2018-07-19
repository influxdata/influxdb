import React, {Component} from 'react'
import PropTypes from 'prop-types'

import OnClickOutside from 'shared/components/OnClickOutside'
import ConfirmOrCancel from 'shared/components/ConfirmOrCancel'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class ChangePassRow extends Component {
  constructor(props) {
    super(props)
    this.state = {
      showForm: false,
    }
  }

  showForm = () => {
    this.setState({showForm: true})
  }

  handleCancel = () => {
    this.setState({showForm: false})
  }

  handleClickOutside() {
    this.setState({showForm: false})
  }

  handleSubmit = user => {
    this.props.onApply(user)
    this.setState({showForm: false})
  }

  handleKeyPress = user => {
    return e => {
      if (e.key === 'Enter') {
        this.handleSubmit(user)
      }
    }
  }

  handleEdit = user => {
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
          <ConfirmOrCancel
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
