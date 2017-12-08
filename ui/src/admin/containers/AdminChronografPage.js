import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import * as adminChronografActionCreators from 'src/admin/actions/chronograf'
import {publishAutoDismissingNotification} from 'shared/dispatchers'

import AdminTabs from 'src/admin/components/chronograf/AdminTabs'
import FancyScrollbar from 'shared/components/FancyScrollbar'

class AdminChronografPage extends Component {
  // TODO: revisit this, possibly don't call setState if both are deep equal
  componentWillReceiveProps(nextProps) {
    const {meCurrentOrganization} = nextProps

    const hasChangedCurrentOrganization =
      meCurrentOrganization.id !== this.props.meCurrentOrganization.id

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
    const {users, meCurrentOrganization, meRole, meID, notify} = this.props

    return (
      <div className="page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1 className="page-header__title">Chronograf Admin</h1>
            </div>
          </div>
        </div>
        <FancyScrollbar className="page-contents">
          {users
            ? <div className="container-fluid">
                <div className="row">
                  <AdminTabs
                    meRole={meRole}
                    meID={meID}
                    // UsersTable
                    users={users}
                    organization={meCurrentOrganization}
                    onCreateUser={this.handleCreateUser}
                    onUpdateUserRole={this.handleUpdateUserRole}
                    onUpdateUserSuperAdmin={this.handleUpdateUserSuperAdmin}
                    onDeleteUser={this.handleDeleteUser}
                    notify={notify}
                  />
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
  meCurrentOrganization: shape({
    id: string.isRequired,
    name: string.isRequired,
  }).isRequired,
  meRole: string.isRequired,
  me: shape({
    name: string.isRequired,
    id: string.isRequired,
  }).isRequired,
  meID: string.isRequired,
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
  auth: {
    me,
    me: {currentOrganization: meCurrentOrganization, role: meRole, id: meID},
  },
}) => ({
  links,
  users,
  meCurrentOrganization,
  meRole,
  me,
  meID,
})

const mapDispatchToProps = dispatch => ({
  actions: bindActionCreators(adminChronografActionCreators, dispatch),
  notify: bindActionCreators(publishAutoDismissingNotification, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(AdminChronografPage)
