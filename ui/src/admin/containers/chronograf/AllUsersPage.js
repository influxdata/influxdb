import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import * as adminChronografActionCreators from 'src/admin/actions/chronograf'
import * as configActionCreators from 'shared/actions/config'
import {publishAutoDismissingNotification} from 'shared/dispatchers'

import AllUsersTableEmpty from 'src/admin/components/chronograf/AllUsersTableEmpty'
import AllUsersTable from 'src/admin/components/chronograf/AllUsersTable'

class AllUsersPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isLoading: true,
    }
  }

  componentDidMount() {
    const {links, actionsConfig: {getAuthConfigAsync}} = this.props
    getAuthConfigAsync(links.config.auth)
  }

  handleCreateUser = user => {
    const {links, actionsAdmin: {createUserAsync}} = this.props
    createUserAsync(links.users, user)
  }

  handleUpdateUserRoles = (user, roles, successMessage) => {
    const {actionsAdmin: {updateUserAsync}} = this.props
    const updatedUser = {...user, roles}
    updateUserAsync(user, updatedUser, successMessage)
  }

  handleUpdateUserSuperAdmin = (user, superAdmin) => {
    const {actionsAdmin: {updateUserAsync}} = this.props
    const updatedUser = {...user, superAdmin}
    updateUserAsync(
      user,
      updatedUser,
      `${user.name}'s SuperAdmin status has been updated`
    )
  }

  handleDeleteUser = user => {
    const {actionsAdmin: {deleteUserAsync}} = this.props
    deleteUserAsync(user, {isAbsoluteDelete: true})
  }

  async componentWillMount() {
    const {
      links,
      actionsAdmin: {loadOrganizationsAsync, loadUsersAsync},
    } = this.props

    this.setState({isLoading: true})

    await Promise.all([
      loadOrganizationsAsync(links.organizations),
      loadUsersAsync(links.allUsers),
    ])

    this.setState({isLoading: false})
  }

  render() {
    const {
      organizations,
      meID,
      users,
      authConfig,
      actionsConfig,
      links,
      notify,
    } = this.props
    const {isLoading} = this.state

    if (isLoading) {
      return <AllUsersTableEmpty />
    }

    return (
      <AllUsersTable
        meID={meID}
        users={users}
        organizations={organizations}
        onCreateUser={this.handleCreateUser}
        onUpdateUserRoles={this.handleUpdateUserRoles}
        onUpdateUserSuperAdmin={this.handleUpdateUserSuperAdmin}
        onDeleteUser={this.handleDeleteUser}
        links={links}
        authConfig={authConfig}
        actionsConfig={actionsConfig}
        notify={notify}
      />
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

AllUsersPage.propTypes = {
  links: shape({
    users: string.isRequired,
    config: shape({
      auth: string.isRequired,
    }).isRequired,
  }),
  meID: string.isRequired,
  users: arrayOf(shape),
  organizations: arrayOf(shape),
  actionsAdmin: shape({
    loadUsersAsync: func.isRequired,
    loadOrganizationsAsync: func.isRequired,
    createUserAsync: func.isRequired,
    updateUserAsync: func.isRequired,
    deleteUserAsync: func.isRequired,
  }),
  actionsConfig: shape({
    getAuthConfigAsync: func.isRequired,
    updateAuthConfigAsync: func.isRequired,
  }),
  authConfig: shape({
    superAdminNewUsers: bool,
  }),
  notify: func.isRequired,
}

const mapStateToProps = ({
  links,
  adminChronograf: {organizations, users},
  config: {auth: authConfig},
}) => ({
  links,
  organizations,
  users,
  authConfig,
})

const mapDispatchToProps = dispatch => ({
  actionsAdmin: bindActionCreators(adminChronografActionCreators, dispatch),
  actionsConfig: bindActionCreators(configActionCreators, dispatch),
  notify: bindActionCreators(publishAutoDismissingNotification, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(AllUsersPage)
