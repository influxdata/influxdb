import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import * as adminChronografActionCreators from 'src/admin/actions/chronograf'
import * as configActionCreators from 'shared/actions/config'
import {getMeAsync} from 'shared/actions/auth'

import OrganizationsTable from 'src/admin/components/chronograf/OrganizationsTable'

class OrganizationsPage extends Component {
  componentDidMount() {
    const {
      links,
      actionsAdmin: {loadOrganizationsAsync},
      actionsConfig: {getAuthConfigAsync},
    } = this.props
    loadOrganizationsAsync(links.organizations)
    getAuthConfigAsync(links.config)
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

  handleUpdateAuthConfig = updatedAuthConfig => {
    const {
      actionsConfig: {updateAuthConfigAsync},
      authConfig,
      links,
    } = this.props
    updateAuthConfigAsync(links.config, authConfig, updatedAuthConfig)
  }

  render() {
    const {organizations, currentOrganization, authConfig} = this.props

    return (
      <OrganizationsTable
        organizations={organizations}
        currentOrganization={currentOrganization}
        onCreateOrg={this.handleCreateOrganization}
        onDeleteOrg={this.handleDeleteOrganization}
        onRenameOrg={this.handleRenameOrganization}
        onTogglePublic={this.handleTogglePublic}
        onChooseDefaultRole={this.handleChooseDefaultRole}
        authConfig={authConfig}
        onUpdateAuthConfig={this.handleUpdateAuthConfig}
      />
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

OrganizationsPage.propTypes = {
  links: shape({
    organizations: string.isRequired,
    config: string.isRequired,
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
  actionsConfig: shape({
    getAuthConfigAsync: func.isRequired,
    updateAuthConfigAsync: func.isRequired,
  }),
  getMe: func.isRequired,
  currentOrganization: shape({
    name: string.isRequired,
    id: string.isRequired,
  }),
  authConfig: shape({
    superAdminFirstUserOnly: bool,
  }),
}

const mapStateToProps = ({
  links,
  adminChronograf: {organizations},
  config: {auth: authConfig},
}) => ({
  links,
  organizations,
  authConfig,
})

const mapDispatchToProps = dispatch => ({
  actionsAdmin: bindActionCreators(adminChronografActionCreators, dispatch),
  actionsConfig: bindActionCreators(configActionCreators, dispatch),
  getMe: bindActionCreators(getMeAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(OrganizationsPage)
