import React, {Component, PropTypes} from 'react'

import ConfirmButtons from 'shared/components/ConfirmButtons'
import Dropdown from 'shared/components/Dropdown'

import {USER_ROLES} from 'src/admin/constants/dummyUsers'
import {MEMBER_ROLE} from 'src/auth/Authorized'

class NewOrganization extends Component {
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
    this.setState({name: e.target.value.trim()})
  }

  handleInputFocus = e => {
    e.target.select()
  }

  handleClickSave = () => {
    const {onCreateOrganization} = this.props
    const {name, defaultRole} = this.state

    onCreateOrganization(name, defaultRole)
  }

  handleChooseDefaultRole = role => {
    this.setState({defaultRole: role.name})
  }

  render() {
    const {name, defaultRole} = this.state
    const {onCancelCreateOrganization} = this.props

    const isSaveDisabled = name === null || name === ''

    const defaultRoleItems = USER_ROLES.map(role => ({
      ...role,
      text: role.name,
    }))

    return (
      <div className="orgs-table--org orgs-table--new-org">
        <div className="orgs-table--id">&mdash;</div>
        <input
          type="text"
          className="form-control input-sm orgs-table--input"
          value={name}
          onKeyDown={this.handleKeyDown}
          onChange={this.handleInputChange}
          onFocus={this.handleInputFocus}
          placeholder="Name this Organization..."
          autoFocus={true}
          ref={r => (this.inputRef = r)}
        />
        <div className="orgs-table--default-role editing">
          <Dropdown
            items={defaultRoleItems}
            onChoose={this.handleChooseDefaultRole}
            selected={defaultRole}
            className="dropdown-stretch"
          />
        </div>
        <ConfirmButtons
          disabled={isSaveDisabled}
          onCancel={onCancelCreateOrganization}
          onConfirm={this.handleClickSave}
        />
      </div>
    )
  }
}

const {func} = PropTypes

NewOrganization.propTypes = {
  onCreateOrganization: func.isRequired,
  onCancelCreateOrganization: func.isRequired,
}

export default NewOrganization
