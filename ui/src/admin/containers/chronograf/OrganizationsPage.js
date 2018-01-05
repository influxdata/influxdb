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
    getAuthConfigAsync(links.config.auth)
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

  handleUpdateAuthConfig = fieldName => updatedValue => {
    const {
      actionsConfig: {updateAuthConfigAsync},
      authConfig,
      links,
    } = this.props
    const updatedAuthConfig = {
      ...authConfig,
      [fieldName]: updatedValue,
    }
    updateAuthConfigAsync(links.config.auth, authConfig, updatedAuthConfig)
  }

  render() {
    const {meCurrentOrganization, organizations, authConfig, me} = this.props

    const organization = organizations.find(
      o => o.id === meCurrentOrganization.id
    )

    return organizations.length
      ? <OrganizationsTable
          organizations={organizations}
          currentOrganization={organization}
          onCreateOrg={this.handleCreateOrganization}
          onDeleteOrg={this.handleDeleteOrganization}
          onRenameOrg={this.handleRenameOrganization}
          onTogglePublic={this.handleTogglePublic}
          onChooseDefaultRole={this.handleChooseDefaultRole}
          authConfig={authConfig}
          onChangeAuthConfig={this.handleUpdateAuthConfig}
          me={me}
        />
      : <div className="page-spinner" />
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

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
  actionsConfig: shape({
    getAuthConfigAsync: func.isRequired,
    updateAuthConfigAsync: func.isRequired,
  }),
  getMe: func.isRequired,
  meCurrentOrganization: shape({
    name: string.isRequired,
    id: string.isRequired,
  }),
  authConfig: shape({
    superAdminNewUsers: bool,
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
  config: {auth: authConfig},
  auth: {me},
}) => ({
  links,
  organizations,
  authConfig,
  me,
})

const mapDispatchToProps = dispatch => ({
  actionsAdmin: bindActionCreators(adminChronografActionCreators, dispatch),
  actionsConfig: bindActionCreators(configActionCreators, dispatch),
  getMe: bindActionCreators(getMeAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(OrganizationsPage)
