import React, {Component} from 'react'
import PropTypes from 'prop-types'

import {ROLES_TABLE} from 'src/admin/constants/tableSizing'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class RoleEditingRow extends Component {
  constructor(props) {
    super(props)
  }

  handleKeyPress = role => {
    return e => {
      if (e.key === 'Enter') {
        this.props.onSave(role)
      }
    }
  }

  handleEdit = role => {
    return e => {
      this.props.onEdit(role, {[e.target.name]: e.target.value})
    }
  }

  render() {
    const {role} = this.props
    return (
      <td style={{width: `${ROLES_TABLE.colName}px`}}>
        <input
          className="form-control input-xs"
          name="name"
          type="text"
          value={role.name || ''}
          placeholder="Role name"
          onChange={this.handleEdit(role)}
          onKeyPress={this.handleKeyPress(role)}
          autoFocus={true}
          spellCheck={false}
          autoComplete="false"
        />
      </td>
    )
  }
}

const {bool, func, shape} = PropTypes

RoleEditingRow.propTypes = {
  role: shape().isRequired,
  isNew: bool,
  onEdit: func.isRequired,
  onSave: func.isRequired,
}

export default RoleEditingRow
