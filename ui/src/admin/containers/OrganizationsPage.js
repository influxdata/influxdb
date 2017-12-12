import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import * as adminChronografActionCreators from 'src/admin/actions/chronograf'
import {getMeAsync} from 'shared/actions/auth'

import OrganizationsTable from 'src/admin/components/chronograf/OrganizationsTable'

class OrganizationsPage extends Component {
  componentDidMount() {
    const {
      links,
      actions: {loadOrganizationsAsync, getAuthSettingsAsync},
    } = this.props
    loadOrganizationsAsync(links.organizations)
    getAuthSettingsAsync(links.config)
  }

  handleCreateOrganization = async organization => {
    const {links, actions: {createOrganizationAsync}} = this.props
    await createOrganizationAsync(links.organizations, organization)
    this.refreshMe()
  }

  handleRenameOrganization = async (organization, name) => {
    const {actions: {updateOrganizationAsync}} = this.props
    await updateOrganizationAsync(organization, {...organization, name})
    this.refreshMe()
  }

  handleDeleteOrganization = organization => {
    const {actions: {deleteOrganizationAsync}} = this.props
    deleteOrganizationAsync(organization)
    this.refreshMe()
  }

  refreshMe = () => {
    const {getMe} = this.props
    getMe({shouldResetMe: false})
  }

  handleTogglePublic = organization => {
    const {actions: {updateOrganizationAsync}} = this.props
    updateOrganizationAsync(organization, {
      ...organization,
      public: !organization.public,
    })
  }

  handleChooseDefaultRole = (organization, defaultRole) => {
    const {actions: {updateOrganizationAsync}} = this.props
    updateOrganizationAsync(organization, {...organization, defaultRole})
    // refreshMe is here to update the org's defaultRole in `me.organizations`
    this.refreshMe()
  }

  handleUpdateAuthSettings = updatedAuthSettings => {
    const {actions: {updateAuthSettingsAsync}, authSettings, links} = this.props
    updateAuthSettingsAsync(links.config, authSettings, updatedAuthSettings)
  }

  render() {
    const {organizations, currentOrganization, authSettings} = this.props

    return (
      <OrganizationsTable
        organizations={organizations}
        currentOrganization={currentOrganization}
        onCreateOrg={this.handleCreateOrganization}
        onDeleteOrg={this.handleDeleteOrganization}
        onRenameOrg={this.handleRenameOrganization}
        onTogglePublic={this.handleTogglePublic}
        onChooseDefaultRole={this.handleChooseDefaultRole}
        authSettings={authSettings}
        onUpdateAuthSettings={this.handleUpdateAuthSettings}
      />
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

OrganizationsPage.propTypes = {
  links: shape({
    organizations: string.isRequired,
    application: string.isRequired,
  }),
  organizations: arrayOf(
    shape({
      id: string, // when optimistically created, it will not have an id
      name: string.isRequired,
      link: string,
    })
  ),
  actions: shape({
    loadOrganizationsAsync: func.isRequired,
    createOrganizationAsync: func.isRequired,
    updateOrganizationAsync: func.isRequired,
    deleteOrganizationAsync: func.isRequired,
    getAuthSettingsAsync: func.isRequired,
    updateAuthSettingsAsync: func.isRequired,
  }),
  getMe: func.isRequired,
  currentOrganization: shape({
    name: string.isRequired,
    id: string.isRequired,
  }),
  authSettings: shape({
    superAdminFirstUserOnly: bool.isRequired,
  }),
}

const mapStateToProps = ({
  links,
  adminChronograf: {organizations, authSettings},
}) => ({
  links,
  organizations,
  authSettings,
})

const mapDispatchToProps = dispatch => ({
  actions: bindActionCreators(adminChronografActionCreators, dispatch),
  getMe: bindActionCreators(getMeAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(OrganizationsPage)
