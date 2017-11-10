import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import * as adminChronografActionCreators from 'src/admin/actions/chronograf'
import {publishAutoDismissingNotification} from 'shared/dispatchers'

import PageHeader from 'src/admin/components/chronograf/PageHeader'
import AdminTabs from 'src/admin/components/chronograf/AdminTabs'
import FancyScrollbar from 'shared/components/FancyScrollbar'

class AdminChronografPage extends Component {
  // TODO: revisit this, possibly don't call setState if both are deep equal
  componentWillReceiveProps(nextProps) {
    const {currentOrganization} = nextProps

    const hasChangedCurrentOrganization =
      currentOrganization.id !== this.props.currentOrganization.id

    if (hasChangedCurrentOrganization) {
      this.loadUsers()
    }
  }

  componentDidMount() {
    this.loadUsers()
  }

  loadUsers = () => {
    const {links, actions: {loadUsersAsync}} = this.props

    loadUsersAsync(links.users)
  }

  // SINGLE USER ACTIONS
  handleCreateUser = user => {
    const {links, actions: {createUserAsync}} = this.props

    createUserAsync(links.users, user)
  }

  handleUpdateUserRole = (user, currentRole, {name}) => {
    const {actions: {updateUserAsync}} = this.props

    const updatedRole = {...currentRole, name}
    const newRoles = user.roles.map(
      r => (r.organization === currentRole.organization ? updatedRole : r)
    )

    updateUserAsync(user, {...user, roles: newRoles})
  }

  handleUpdateUserSuperAdmin = (user, superAdmin) => {
    const {actions: {updateUserAsync}} = this.props

    const updatedUser = {...user, superAdmin}

    updateUserAsync(user, updatedUser)
  }

  handleDeleteUser = user => {
    const {actions: {deleteUserAsync}} = this.props

    deleteUserAsync(user)
  }

  render() {
    const {users, currentOrganization, meRole} = this.props

    return (
      <div className="page">
        <PageHeader currentOrganization={currentOrganization} />
        <FancyScrollbar className="page-contents">
          {users
            ? <div className="container-fluid">
                <div className="row">
                  <div className="col-xs-12">
                    <AdminTabs
                      meRole={meRole}
                      // UsersTable
                      users={users}
                      organization={currentOrganization}
                      onCreateUser={this.handleCreateUser}
                      onUpdateUserRole={this.handleUpdateUserRole}
                      onUpdateUserSuperAdmin={this.handleUpdateUserSuperAdmin}
                      onDeleteUser={this.handleDeleteUser}
                    />
                  </div>
                </div>
              </div>
            : <div className="page-spinner" />}
        </FancyScrollbar>
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

AdminChronografPage.propTypes = {
  links: shape({
    users: string.isRequired,
  }),
  users: arrayOf(shape),
  currentOrganization: shape({
    id: string.isRequired,
    name: string.isRequired,
  }).isRequired,
  meRole: string.isRequired,
  actions: shape({
    loadUsersAsync: func.isRequired,
    createUserAsync: func.isRequired,
    updateUserAsync: func.isRequired,
    deleteUserAsync: func.isRequired,
  }),
  notify: func.isRequired,
}

const mapStateToProps = ({
  links,
  adminChronograf: {users},
  auth: {me: {currentOrganization, role: meRole}},
}) => ({
  links,
  users,
  currentOrganization,
  meRole,
})

const mapDispatchToProps = dispatch => ({
  actions: bindActionCreators(adminChronografActionCreators, dispatch),
  notify: bindActionCreators(publishAutoDismissingNotification, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(AdminChronografPage)
