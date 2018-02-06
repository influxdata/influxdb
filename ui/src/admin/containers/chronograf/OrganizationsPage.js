import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import * as adminChronografActionCreators from 'src/admin/actions/chronograf'
import {getMeAsync} from 'shared/actions/auth'

import OrganizationsTable from 'src/admin/components/chronograf/OrganizationsTable'

class OrganizationsPage extends Component {
  componentDidMount() {
    const {links, actionsAdmin: {loadOrganizationsAsync}} = this.props
    loadOrganizationsAsync(links.organizations)
  }

  handleCreateOrganization = async organization => {
    const {links, actionsAdmin: {createOrganizationAsync}} = this.props
    await createOrganizationAsync(links.organizations, organization)
    this.refreshMe()
  }

  handleRenameOrganization = async (organization, name) => {
    const {actionsAdmin: {updateOrganizationAsync}} = this.props
    await updateOrganizationAsync(organization, {...organization, name})
    this.refreshMe()
  }

  handleDeleteOrganization = organization => {
    const {actionsAdmin: {deleteOrganizationAsync}} = this.props
    deleteOrganizationAsync(organization)
    this.refreshMe()
  }

  refreshMe = () => {
    const {getMe} = this.props
    getMe({shouldResetMe: false})
  }

  handleTogglePublic = organization => {
    const {actionsAdmin: {updateOrganizationAsync}} = this.props
    updateOrganizationAsync(organization, {
      ...organization,
      public: !organization.public,
    })
  }

  handleChooseDefaultRole = (organization, defaultRole) => {
    const {actionsAdmin: {updateOrganizationAsync}} = this.props
    updateOrganizationAsync(organization, {...organization, defaultRole})
    // refreshMe is here to update the org's defaultRole in `me.organizations`
    this.refreshMe()
  }

  render() {
    const {meCurrentOrganization, organizations, me} = this.props

    const organization = organizations.find(
      o => o.id === meCurrentOrganization.id
    )

    return (
      <OrganizationsTable
        organizations={organizations}
        currentOrganization={organization}
        onCreateOrg={this.handleCreateOrganization}
        onDeleteOrg={this.handleDeleteOrganization}
        onRenameOrg={this.handleRenameOrganization}
        onTogglePublic={this.handleTogglePublic}
        onChooseDefaultRole={this.handleChooseDefaultRole}
        me={me}
      />
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

OrganizationsPage.propTypes = {
  links: shape({
    organizations: string.isRequired,
    config: shape({
      auth: string.isRequired,
    }).isRequired,
  }),
  organizations: arrayOf(
    shape({
      id: string, // when optimistically created, it will not have an id
      name: string.isRequired,
      link: string,
    })
  ),
  actionsAdmin: shape({
    loadOrganizationsAsync: func.isRequired,
    createOrganizationAsync: func.isRequired,
    updateOrganizationAsync: func.isRequired,
    deleteOrganizationAsync: func.isRequired,
  }),
  getMe: func.isRequired,
  meCurrentOrganization: shape({
    name: string.isRequired,
    id: string.isRequired,
  }),
  me: shape({
    organizations: arrayOf(
      shape({
        id: string.isRequired,
        name: string.isRequired,
        defaultRole: string.isRequired,
      })
    ),
  }),
}

const mapStateToProps = ({
  links,
  adminChronograf: {organizations},
  auth: {me},
}) => ({
  links,
  organizations,
  me,
})

const mapDispatchToProps = dispatch => ({
  actionsAdmin: bindActionCreators(adminChronografActionCreators, dispatch),
  getMe: bindActionCreators(getMeAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(OrganizationsPage)
