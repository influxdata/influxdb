import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {
  addUser,
  addRole,
  editUser,
  editRole,
  deleteUser,
  deleteRole,
  loadUsersAsync,
  loadRolesAsync,
  createUserAsync,
  createRoleAsync,
  deleteUserAsync,
  deleteRoleAsync,
  loadPermissionsAsync,
  updateRoleUsersAsync,
  updateUserRolesAsync,
  updateUserPasswordAsync,
  updateRolePermissionsAsync,
  updateUserPermissionsAsync,
  filterUsers as filterUsersAction,
  filterRoles as filterRolesAction,
} from 'src/admin/actions'

import AdminTabs from 'src/admin/components/AdminTabs'
import SourceIndicator from 'shared/components/SourceIndicator'
import FancyScrollbar from 'shared/components/FancyScrollbar'

import {publishAutoDismissingNotification} from 'shared/dispatchers'

const isValidUser = user => {
  const minLen = 3
  return user.name.length >= minLen && user.password.length >= minLen
}

const isValidRole = role => {
  const minLen = 3
  return role.name.length >= minLen
}

class AdminChronografPage extends Component {
  constructor(props) {
    super(props)
  }

  componentDidMount() {
    const {source, loadUsers, loadRoles, loadPermissions} = this.props

    loadUsers(source.links.users)
    loadPermissions(source.links.permissions)
    if (source.links.roles) {
      loadRoles(source.links.roles)
    }
  }

  handleClickCreate = type => () => {
    if (type === 'users') {
      this.props.addUser()
    } else if (type === 'roles') {
      this.props.addRole()
    }
  }

  handleEditUser = (user, updates) => {
    this.props.editUser(user, updates)
  }

  handleEditRole = (role, updates) => {
    this.props.editRole(role, updates)
  }

  handleSaveUser = async user => {
    const {notify} = this.props
    if (!isValidUser(user)) {
      notify('error', 'Username and/or password too short')
      return
    }
    if (user.isNew) {
      this.props.createUser(this.props.source.links.users, user)
    } else {
      // TODO update user
    }
  }

  handleSaveRole = async role => {
    const {notify} = this.props
    if (!isValidRole(role)) {
      notify('error', 'Role name too short')
      return
    }
    if (role.isNew) {
      this.props.createRole(this.props.source.links.roles, role)
    } else {
      // TODO update role
    }
  }

  handleCancelEditUser = user => {
    this.props.removeUser(user)
  }

  handleCancelEditRole = role => {
    this.props.removeRole(role)
  }

  handleDeleteRole = role => {
    this.props.deleteRole(role)
  }

  handleDeleteUser = user => {
    this.props.deleteUser(user)
  }

  handleUpdateRoleUsers = (role, users) => {
    this.props.updateRoleUsers(role, users)
  }

  handleUpdateRolePermissions = (role, permissions) => {
    this.props.updateRolePermissions(role, permissions)
  }

  handleUpdateUserPermissions = (user, permissions) => {
    this.props.updateUserPermissions(user, permissions)
  }

  handleUpdateUserRoles = (user, roles) => {
    this.props.updateUserRoles(user, roles)
  }

  handleUpdateUserPassword = (user, password) => {
    this.props.updateUserPassword(user, password)
  }

  render() {
    const {
      users,
      roles,
      source,
      permissions,
      filterUsers,
      filterRoles,
    } = this.props
    const hasRoles = !!source.links.roles
    const globalPermissions = permissions.find(p => p.scope === 'all')
    const allowed = globalPermissions ? globalPermissions.allowed : []

    return (
      <div className="page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1 className="page-header__title">Chronograf Admin</h1>
            </div>
            <div className="page-header__right">
              <SourceIndicator />
            </div>
          </div>
        </div>
        <FancyScrollbar className="page-contents">
          {users
            ? <div className="container-fluid">
                <div className="row">
                  <AdminTabs
                    users={users}
                    roles={roles}
                    source={source}
                    hasRoles={hasRoles}
                    permissions={allowed}
                    onFilterUsers={filterUsers}
                    onFilterRoles={filterRoles}
                    onEditUser={this.handleEditUser}
                    onEditRole={this.handleEditRole}
                    onSaveUser={this.handleSaveUser}
                    onSaveRole={this.handleSaveRole}
                    onDeleteUser={this.handleDeleteUser}
                    onDeleteRole={this.handleDeleteRole}
                    onClickCreate={this.handleClickCreate}
                    onCancelEditUser={this.handleCancelEditUser}
                    onCancelEditRole={this.handleCancelEditRole}
                    isEditingUsers={users.some(u => u.isEditing)}
                    isEditingRoles={roles.some(r => r.isEditing)}
                    onUpdateRoleUsers={this.handleUpdateRoleUsers}
                    onUpdateUserRoles={this.handleUpdateUserRoles}
                    onUpdateUserPassword={this.handleUpdateUserPassword}
                    onUpdateRolePermissions={this.handleUpdateRolePermissions}
                    onUpdateUserPermissions={this.handleUpdateUserPermissions}
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
  source: shape({
    id: string.isRequired,
    links: shape({
      users: string.isRequired,
    }),
  }).isRequired,
  users: arrayOf(shape()),
  roles: arrayOf(shape()),
  permissions: arrayOf(shape()),
  loadUsers: func,
  loadRoles: func,
  loadPermissions: func,
  addUser: func,
  addRole: func,
  removeUser: func,
  removeRole: func,
  editUser: func,
  editRole: func,
  createUser: func,
  createRole: func,
  deleteRole: func,
  deleteUser: func,
  filterRoles: func,
  filterUsers: func,
  updateRoleUsers: func,
  updateRolePermissions: func,
  updateUserPermissions: func,
  updateUserRoles: func,
  updateUserPassword: func,
  notify: func,
}

const mapStateToProps = ({admin: {users, roles, permissions}}) => ({
  users,
  roles,
  permissions,
})

const mapDispatchToProps = dispatch => ({
  loadUsers: bindActionCreators(loadUsersAsync, dispatch),
  loadRoles: bindActionCreators(loadRolesAsync, dispatch),
  loadPermissions: bindActionCreators(loadPermissionsAsync, dispatch),
  addUser: bindActionCreators(addUser, dispatch),
  addRole: bindActionCreators(addRole, dispatch),
  removeUser: bindActionCreators(deleteUser, dispatch),
  removeRole: bindActionCreators(deleteRole, dispatch),
  editUser: bindActionCreators(editUser, dispatch),
  editRole: bindActionCreators(editRole, dispatch),
  createUser: bindActionCreators(createUserAsync, dispatch),
  createRole: bindActionCreators(createRoleAsync, dispatch),
  deleteUser: bindActionCreators(deleteUserAsync, dispatch),
  deleteRole: bindActionCreators(deleteRoleAsync, dispatch),
  filterUsers: bindActionCreators(filterUsersAction, dispatch),
  filterRoles: bindActionCreators(filterRolesAction, dispatch),
  updateRoleUsers: bindActionCreators(updateRoleUsersAsync, dispatch),
  updateRolePermissions: bindActionCreators(
    updateRolePermissionsAsync,
    dispatch
  ),
  updateUserPermissions: bindActionCreators(
    updateUserPermissionsAsync,
    dispatch
  ),
  updateUserRoles: bindActionCreators(updateUserRolesAsync, dispatch),
  updateUserPassword: bindActionCreators(updateUserPasswordAsync, dispatch),
  notify: bindActionCreators(publishAutoDismissingNotification, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(AdminChronografPage)
