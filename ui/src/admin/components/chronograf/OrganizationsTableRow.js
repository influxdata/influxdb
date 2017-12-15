import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {withRouter} from 'react-router'

import SlideToggle from 'shared/components/SlideToggle'
import ConfirmButtons from 'shared/components/ConfirmButtons'
import Dropdown from 'shared/components/Dropdown'

import {meChangeOrganizationAsync} from 'shared/actions/auth'

import {DEFAULT_ORG_ID} from 'src/admin/constants/chronografAdmin'
import {USER_ROLES} from 'src/admin/constants/chronografAdmin'

const OrganizationsTableRowDeleteButton = ({organization, onClickDelete}) =>
  organization.id === DEFAULT_ORG_ID
    ? <button
        className="btn btn-sm btn-default btn-square orgs-table--delete"
        disabled={true}
      >
        <span className="icon trash" />
      </button>
    : <button
        className="btn btn-sm btn-default btn-square"
        onClick={onClickDelete}
      >
        <span className="icon trash" />
      </button>

class OrganizationsTableRow extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isEditing: false,
      isDeleting: false,
      workingName: this.props.organization.name,
    }
  }

  handleChangeCurrentOrganization = async () => {
    const {
      router,
      links,
      meChangeOrganization,
      organization,
      userHasRoleInOrg,
    } = this.props

    await meChangeOrganization(
      links.me,
      {organization: organization.id},
      {userHasRoleInOrg}
    )
    router.push('')
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

  handleTogglePublic = () => {
    const {organization, onTogglePublic} = this.props
    onTogglePublic(organization)
  }

  handleChooseDefaultRole = role => {
    const {organization, onChooseDefaultRole} = this.props
    onChooseDefaultRole(organization, role.name)
  }

  render() {
    const {workingName, isEditing, isDeleting} = this.state
    const {organization, currentOrganization} = this.props

    const dropdownRolesItems = USER_ROLES.map(role => ({
      ...role,
      text: role.name,
    }))

    const defaultRoleClassName = isDeleting
      ? 'orgs-table--default-role editing'
      : 'orgs-table--default-role'

    return (
      <div className="orgs-table--org">
        <div className="orgs-table--active">
          {organization.id === currentOrganization.id
            ? <button className="btn btn-sm btn-success">
                <span className="icon checkmark" /> Current
              </button>
            : <button
                className="btn btn-sm btn-default"
                onClick={this.handleChangeCurrentOrganization}
              >
                <span className="icon shuffle" /> Switch to
              </button>}
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
        {organization.id === DEFAULT_ORG_ID
          ? <div className="orgs-table--public">
              <SlideToggle
                size="xs"
                active={organization.public}
                onToggle={this.handleTogglePublic}
              />
            </div>
          : <div className="orgs-table--public disabled">&mdash;</div>}
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
          : <OrganizationsTableRowDeleteButton
              organization={organization}
              onClickDelete={this.handleDeleteClick}
            />}
      </div>
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

OrganizationsTableRow.propTypes = {
  organization: shape({
    id: string, // when optimistically created, organization will not have an id
    name: string.isRequired,
    defaultRole: string.isRequired,
  }).isRequired,
  onDelete: func.isRequired,
  onRename: func.isRequired,
  onTogglePublic: func.isRequired,
  onChooseDefaultRole: func.isRequired,
  currentOrganization: shape({
    name: string.isRequired,
    id: string.isRequired,
  }),
  router: shape({
    push: func.isRequired,
  }).isRequired,
  links: shape({
    me: string,
    external: shape({
      custom: arrayOf(
        shape({
          name: string.isRequired,
          url: string.isRequired,
        })
      ),
    }),
  }),
  meChangeOrganization: func.isRequired,
  userHasRoleInOrg: bool.isRequired,
}

OrganizationsTableRowDeleteButton.propTypes = {
  organization: shape({
    id: string, // when optimistically created, organization will not have an id
    name: string.isRequired,
    defaultRole: string.isRequired,
  }).isRequired,
  onClickDelete: func.isRequired,
}

const mapDispatchToProps = dispatch => ({
  meChangeOrganization: bindActionCreators(meChangeOrganizationAsync, dispatch),
})

const mapStateToProps = ({links}) => ({
  links,
})

export default connect(mapStateToProps, mapDispatchToProps)(
  withRouter(OrganizationsTableRow)
)
