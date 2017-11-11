import React, {Component, PropTypes} from 'react'

import ConfirmButtons from 'shared/components/ConfirmButtons'
import Dropdown from 'shared/components/Dropdown'

import {USER_ROLES} from 'src/admin/constants/dummyUsers'

class OrganizationsTableRow extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isEditing: false,
      isDeleting: false,
      workingName: this.props.organization.name,
    }
  }

  handleNameClick = () => {
    this.setState({isEditing: true})
  }

  handleConfirmRename = () => {
    const {onRename, organization} = this.props
    const {workingName} = this.state

    onRename(organization, workingName)
    this.setState({workingName, isEditing: false})
  }

  handleCancelRename = () => {
    const {organization} = this.props

    this.setState({
      workingName: organization.name,
      isEditing: false,
    })
  }

  handleInputChange = e => {
    this.setState({workingName: e.target.value})
  }

  handleInputBlur = () => {
    const {organization} = this.props
    const {workingName} = this.state

    if (organization.name === workingName) {
      this.handleCancelRename()
    } else {
      this.handleConfirmRename()
    }
  }

  handleKeyDown = e => {
    if (e.key === 'Enter') {
      this.handleInputBlur()
    } else if (e.key === 'Escape') {
      this.handleCancelRename()
    }
  }

  handleFocus = e => {
    e.target.select()
  }

  handleDeleteClick = () => {
    this.setState({isDeleting: true})
  }

  handleDismissDeleteConfirmation = () => {
    this.setState({isDeleting: false})
  }

  handleDeleteOrg = organization => {
    const {onDelete} = this.props
    onDelete(organization)
  }

  handleChooseDefaultRole = role => {
    const {organization, onChooseDefaultRole} = this.props
    onChooseDefaultRole(organization, role.name)
  }

  render() {
    const {workingName, isEditing, isDeleting} = this.state
    const {organization} = this.props

    const dropdownRolesItems = USER_ROLES.map(role => ({
      ...role,
      text: role.name,
    }))

    const defaultRoleClassName = isDeleting
      ? 'orgs-table--default-role editing'
      : 'orgs-table--default-role'

    return (
      <div className="orgs-table--org">
        <div className="orgs-table--id">
          {organization.id}
        </div>
        {isEditing
          ? <input
              type="text"
              className="form-control input-sm orgs-table--input"
              defaultValue={workingName}
              onChange={this.handleInputChange}
              onBlur={this.handleInputBlur}
              onKeyDown={this.handleKeyDown}
              placeholder="Name this Organization..."
              autoFocus={true}
              onFocus={this.handleFocus}
              ref={r => (this.inputRef = r)}
            />
          : <div className="orgs-table--name" onClick={this.handleNameClick}>
              {workingName}
              <span className="icon pencil" />
            </div>}
        <div className="orgs-table--whitelist disabled">&mdash;</div>
        <div className={defaultRoleClassName}>
          <Dropdown
            items={dropdownRolesItems}
            onChoose={this.handleChooseDefaultRole}
            selected={organization.defaultRole}
            className="dropdown-stretch"
          />
        </div>
        {isDeleting
          ? <ConfirmButtons
              item={organization}
              onCancel={this.handleDismissDeleteConfirmation}
              onConfirm={this.handleDeleteOrg}
              onClickOutside={this.handleDismissDeleteConfirmation}
              confirmLeft={true}
            />
          : <button
              className="btn btn-sm btn-default btn-square"
              onClick={this.handleDeleteClick}
            >
              <span className="icon trash" />
            </button>}
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

OrganizationsTableRow.propTypes = {
  organization: shape({
    id: string, // when optimistically created, organization will not have an id
    name: string.isRequired,
    defaultRole: string.isRequired,
  }).isRequired,
  onDelete: func.isRequired,
  onRename: func.isRequired,
  onChooseDefaultRole: func.isRequired,
}

export default OrganizationsTableRow
