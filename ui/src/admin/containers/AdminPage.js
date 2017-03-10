import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {
  loadUsersAsync,
  loadRolesAsync,
  loadPermissionsAsync,
  addUser,
  deleteUser, // TODO rename to removeUser throughout + tests
  editUser,
  createUserAsync,
  deleteRoleAsync,
  deleteUserAsync,
  updateRoleUsersAsync,
  updateRolePermissionsAsync,
  filterRoles as filterRolesAction,
  filterUsers as filterUsersAction,
} from 'src/admin/actions'
import AdminTabs from 'src/admin/components/AdminTabs'

const isValid = (user) => {
  const minLen = 3
  return (user.name.length >= minLen && user.password.length >= minLen)
}

class AdminPage extends Component {
  constructor(props) {
    super(props)

    this.handleClickCreate = ::this.handleClickCreate
    this.handleEditUser = ::this.handleEditUser
    this.handleSaveUser = ::this.handleSaveUser
    this.handleCancelEdit = ::this.handleCancelEdit
    this.handleDeleteRole = ::this.handleDeleteRole
    this.handleDeleteUser = ::this.handleDeleteUser
    this.handleUpdateRoleUsers = ::this.handleUpdateRoleUsers
    this.handleUpdateRolePermissions = ::this.handleUpdateRolePermissions
  }

  componentDidMount() {
    const {source, loadUsers, loadRoles, loadPermissions} = this.props

    loadUsers(source.links.users)
    loadPermissions(source.links.permissions)
    if (source.links.roles) {
      loadRoles(source.links.roles)
    }
  }

  handleClickCreate(type) {
    if (type === 'users') {
      this.props.addUser()
    }
  }

  handleEditUser(user, updates) {
    this.props.editUser(user, updates)
  }

  async handleSaveUser(user) {
    if (!isValid(user)) {
      this.props.addFlashMessage({type: 'error', text: 'Username and/or password too short'})
      return
    }
    if (user.isNew) {
      this.props.createUser(this.props.source.links.users, user)
    } else {
      // TODO update user
    }
  }

  handleCancelEdit(user) {
    this.props.removeUser(user)
  }

  handleDeleteRole(role) {
    this.props.deleteRole(role, this.props.addFlashMessage)
  }

  handleDeleteUser(user) {
    this.props.deleteUser(user, this.props.addFlashMessage)
  }

  handleUpdateRoleUsers(role, users) {
    this.props.updateRoleUsers(role, users)
  }

  handleUpdateRolePermissions(role, permissions) {
    this.props.updateRolePermissions(role, permissions)
  }

  render() {
    const {users, roles, source, permissions, filterUsers, filterRoles, addFlashMessage} = this.props
    const globalPermissions = permissions.find((p) => p.scope === 'all')
    const allowed = globalPermissions ? globalPermissions.allowed : []

    return (
      <div className="page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1>
                Admin
              </h1>
            </div>
          </div>
        </div>
        <div className="page-contents">
          <div className="container-fluid">
            <div className="row">
                {
                  users.length ?
                  <AdminTabs
                    users={users}
                    roles={roles}
                    source={source}
                    permissions={allowed}
                    isEditingUsers={users.some(u => u.isEditing)}
                    onClickCreate={this.handleClickCreate}
                    onEditUser={this.handleEditUser}
                    onSaveUser={this.handleSaveUser}
                    onCancelEdit={this.handleCancelEdit}
                    onDeleteRole={this.handleDeleteRole}
                    onDeleteUser={this.handleDeleteUser}
                    onFilterUsers={filterUsers}
                    onFilterRoles={filterRoles}
                    addFlashMessage={addFlashMessage}
                    onUpdateRoleUsers={this.handleUpdateRoleUsers}
                    onUpdateRolePermissions={this.handleUpdateRolePermissions}
                  /> :
                  <span>Loading...</span>
                }
            </div>
          </div>
        </div>
      </div>
    )
  }
}

const {
  arrayOf,
  func,
  shape,
  string,
} = PropTypes

AdminPage.propTypes = {
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
  removeUser: func,
  editUser: func,
  createUser: func,
  deleteRole: func,
  deleteUser: func,
  addFlashMessage: func,
  filterRoles: func,
  filterUsers: func,
  updateRoleUsers: func,
  updateRolePermissions: func,
}

const mapStateToProps = ({admin: {users, roles, permissions}}) => ({
  users,
  roles,
  permissions,
})

const mapDispatchToProps = (dispatch) => ({
  loadUsers: bindActionCreators(loadUsersAsync, dispatch),
  loadRoles: bindActionCreators(loadRolesAsync, dispatch),
  loadPermissions: bindActionCreators(loadPermissionsAsync, dispatch),
  addUser: bindActionCreators(addUser, dispatch),
  removeUser: bindActionCreators(deleteUser, dispatch),
  editUser: bindActionCreators(editUser, dispatch),
  createUser: bindActionCreators(createUserAsync, dispatch),
  deleteRole: bindActionCreators(deleteRoleAsync, dispatch),
  deleteUser: bindActionCreators(deleteUserAsync, dispatch),
  filterRoles: bindActionCreators(filterRolesAction, dispatch),
  filterUsers: bindActionCreators(filterUsersAction, dispatch),
  updateRoleUsers: bindActionCreators(updateRoleUsersAsync, dispatch),
  updateRolePermissions: bindActionCreators(updateRolePermissionsAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(AdminPage)
