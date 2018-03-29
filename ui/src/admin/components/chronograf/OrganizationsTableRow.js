import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {withRouter} from 'react-router'

import ConfirmButton from 'shared/components/ConfirmButton'
import Dropdown from 'shared/components/Dropdown'
import InputClickToEdit from 'shared/components/InputClickToEdit'

import {meChangeOrganizationAsync} from 'shared/actions/auth'

import {DEFAULT_ORG_ID} from 'src/admin/constants/chronografAdmin'
import {USER_ROLES} from 'src/admin/constants/chronografAdmin'

class OrganizationsTableRow extends Component {
  handleChangeCurrentOrganization = async () => {
    const {router, links, meChangeOrganization, organization} = this.props

    await meChangeOrganization(links.me, {organization: organization.id})
    router.push('')
  }
  handleUpdateOrgName = newName => {
    const {organization, onRename} = this.props
    onRename(organization, newName)
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
    const {organization, currentOrganization} = this.props

    const dropdownRolesItems = USER_ROLES.map(role => ({
      ...role,
      text: role.name,
    }))

    return (
      <div className="fancytable--row">
        <div className="fancytable--td orgs-table--active">
          {organization.id === currentOrganization.id ? (
            <button className="btn btn-sm btn-success">
              <span className="icon checkmark" /> Current
            </button>
          ) : (
            <button
              className="btn btn-sm btn-default"
              onClick={this.handleChangeCurrentOrganization}
            >
              <span className="icon shuffle" /> Switch to
            </button>
          )}
        </div>
        <InputClickToEdit
          value={organization.name}
          wrapperClass="fancytable--td orgs-table--name"
          onBlur={this.handleUpdateOrgName}
        />
        <div className="fancytable--td orgs-table--default-role">
          <Dropdown
            items={dropdownRolesItems}
            onChoose={this.handleChooseDefaultRole}
            selected={organization.defaultRole}
            className="dropdown-stretch"
          />
        </div>
        <ConfirmButton
          confirmAction={this.handleDeleteOrg}
          confirmText="Delete Organization?"
          size="btn-sm"
          square={true}
          icon="trash"
          disabled={organization.id === DEFAULT_ORG_ID}
        />
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

const mapDispatchToProps = dispatch => ({
  meChangeOrganization: bindActionCreators(meChangeOrganizationAsync, dispatch),
})

const mapStateToProps = ({links}) => ({
  links,
})

export default connect(mapStateToProps, mapDispatchToProps)(
  withRouter(OrganizationsTableRow)
)
