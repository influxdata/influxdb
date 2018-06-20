import React, {Component} from 'react'
import PropTypes from 'prop-types'
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
} from 'src/admin/actions/influxdb'

import UsersTable from 'src/admin/components/UsersTable'
import RolesTable from 'src/admin/components/RolesTable'
import QueriesPage from 'src/admin/containers/QueriesPage'
import DatabaseManagerPage from 'src/admin/containers/DatabaseManagerPage'
import PageHeader from 'shared/components/PageHeader'
import FancyScrollbar from 'shared/components/FancyScrollbar'
import SubSections from 'shared/components/SubSections'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {notify as notifyAction} from 'shared/actions/notifications'

import {
  notifyRoleNameInvalid,
  notifyDBUserNamePasswordInvalid,
} from 'shared/copy/notifications'

const isValidUser = user => {
  const minLen = 3
  return user.name.length >= minLen && user.password.length >= minLen
}

const isValidRole = role => {
  const minLen = 3
  return role.name.length >= minLen
}

@ErrorHandling
class AdminInfluxDBPage extends Component {
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
      notify(notifyDBUserNamePasswordInvalid())
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
      notify(notifyRoleNameInvalid())
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

  getAdminSubSections = () => {
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

    return [
      {
        url: 'databases',
        name: 'Databases',
        enabled: true,
        component: <DatabaseManagerPage source={source} />,
      },
      {
        url: 'users',
        name: 'Users',
        enabled: true,
        component: (
          <UsersTable
            users={users}
            allRoles={roles}
            hasRoles={hasRoles}
            permissions={allowed}
            isEditing={users.some(u => u.isEditing)}
            onSave={this.handleSaveUser}
            onCancel={this.handleCancelEditUser}
            onClickCreate={this.handleClickCreate}
            onEdit={this.handleEditUser}
            onDelete={this.handleDeleteUser}
            onFilter={filterUsers}
            onUpdatePermissions={this.handleUpdateUserPermissions}
            onUpdateRoles={this.handleUpdateUserRoles}
            onUpdatePassword={this.handleUpdateUserPassword}
          />
        ),
      },
      {
        url: 'roles',
        name: 'Roles',
        enabled: hasRoles,
        component: (
          <RolesTable
            roles={roles}
            allUsers={users}
            permissions={allowed}
            isEditing={roles.some(r => r.isEditing)}
            onClickCreate={this.handleClickCreate}
            onEdit={this.handleEditRole}
            onSave={this.handleSaveRole}
            onCancel={this.handleCancelEditRole}
            onDelete={this.handleDeleteRole}
            onFilter={filterRoles}
            onUpdateRoleUsers={this.handleUpdateRoleUsers}
            onUpdateRolePermissions={this.handleUpdateRolePermissions}
          />
        ),
      },
      {
        url: 'queries',
        name: 'Queries',
        enabled: true,
        component: <QueriesPage source={source} />,
      },
    ]
  }

  render() {
    const {users, source, params} = this.props

    return (
      <div className="page">
        <PageHeader title="InfluxDB Admin" sourceIndicator={true} />
        <FancyScrollbar className="page-contents">
          {users ? (
            <div className="container-fluid">
              <SubSections
                parentUrl="admin-influxdb"
                sourceID={source.id}
                activeSection={params.tab}
                sections={this.getAdminSubSections()}
              />
            </div>
          ) : (
            <div className="page-spinner" />
          )}
        </FancyScrollbar>
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

AdminInfluxDBPage.propTypes = {
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
  notify: func.isRequired,
  params: shape({
    tab: string,
  }).isRequired,
}

const mapStateToProps = ({adminInfluxDB: {users, roles, permissions}}) => ({
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
  notify: bindActionCreators(notifyAction, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(AdminInfluxDBPage)
