import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {withRouter} from 'react-router'

import ConfirmButtons from 'shared/components/ConfirmButtons'
import Dropdown from 'shared/components/Dropdown'
import InputClickToEdit from 'shared/components/InputClickToEdit'

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
      isDeleting: false,
    }
  }

  handleChangeCurrentOrganization = async () => {
    const {router, links, meChangeOrganization, organization} = this.props

    await meChangeOrganization(links.me, {organization: organization.id})
    router.push('')
  }
  handleUpdateOrgName = newName => {
    const {organization, onRename} = this.props
    onRename(organization, newName)
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
    const {isDeleting} = this.state
    const {organization, currentOrganization} = this.props

    const dropdownRolesItems = USER_ROLES.map(role => ({
      ...role,
      text: role.name,
    }))

    const defaultRoleClassName = isDeleting
      ? 'fancytable--td orgs-table--default-role deleting'
      : 'fancytable--td orgs-table--default-role'

    return (
      <div className="fancytable--row">
        <div className="fancytable--td orgs-table--active">
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
        <InputClickToEdit
          value={organization.name}
          wrapperClass="fancytable--td orgs-table--name"
          onUpdate={this.handleUpdateOrgName}
        />
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

const {arrayOf, func, shape, string} = PropTypes

OrganizationsTableRow.propTypes = {
  organization: shape({
    id: string, // when optimistically created, organization will not have an id
    name: string.isRequired,
    defaultRole: string.isRequired,
  }).isRequired,
  onDelete: func.isRequired,
  onRename: func.isRequired,
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
