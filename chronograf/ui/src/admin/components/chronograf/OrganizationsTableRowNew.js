import React, {Component} from 'react'
import PropTypes from 'prop-types'

import ConfirmOrCancel from 'shared/components/ConfirmOrCancel'
import Dropdown from 'shared/components/Dropdown'

import {USER_ROLES} from 'src/admin/constants/chronografAdmin'
import {MEMBER_ROLE} from 'src/auth/Authorized'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class OrganizationsTableRowNew extends Component {
  constructor(props) {
    super(props)

    this.state = {
      name: 'Untitled Organization',
      defaultRole: MEMBER_ROLE,
    }
  }

  handleKeyDown = e => {
    const {onCancelCreateOrganization} = this.props

    if (e.key === 'Escape') {
      onCancelCreateOrganization()
    }
    if (e.key === 'Enter') {
      this.handleClickSave()
    }
  }

  handleInputChange = e => {
    this.setState({name: e.target.value})
  }

  handleInputFocus = e => {
    e.target.select()
  }

  handleClickSave = () => {
    const {onCreateOrganization} = this.props
    const {name, defaultRole} = this.state

    onCreateOrganization({name: name.trim(), defaultRole})
  }

  handleChooseDefaultRole = role => {
    this.setState({defaultRole: role.name})
  }

  render() {
    const {name, defaultRole} = this.state
    const {onCancelCreateOrganization} = this.props

    const isSaveDisabled = name === null || name === ''

    const dropdownRolesItems = USER_ROLES.map(role => ({
      ...role,
      text: role.name,
    }))

    return (
      <div className="fancytable--row">
        <div className="fancytable--td orgs-table--active">&mdash;</div>
        <div className="fancytable--td orgs-table--name">
          <input
            type="text"
            className="form-control input-sm"
            value={name}
            onKeyDown={this.handleKeyDown}
            onChange={this.handleInputChange}
            onFocus={this.handleInputFocus}
            placeholder="Name this Organization..."
            autoFocus={true}
            ref={r => (this.inputRef = r)}
          />
        </div>
        <div className="fancytable--td orgs-table--default-role creating">
          <Dropdown
            items={dropdownRolesItems}
            onChoose={this.handleChooseDefaultRole}
            selected={defaultRole}
            className="dropdown-stretch"
          />
        </div>
        <ConfirmOrCancel
          disabled={isSaveDisabled}
          onCancel={onCancelCreateOrganization}
          onConfirm={this.handleClickSave}
          confirmLeft={true}
        />
      </div>
    )
  }
}

const {func} = PropTypes

OrganizationsTableRowNew.propTypes = {
  onCreateOrganization: func.isRequired,
  onCancelCreateOrganization: func.isRequired,
}

export default OrganizationsTableRowNew
