import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {
  loadUsersAsync,
  loadRolesAsync,
  addUser,
  setEditingMode,
  deleteUser, // TODO rename to removeUser throughout + tests
  editUser,
  createUserAsync,
  deleteRoleAsync,
  deleteUserAsync,
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
  }

  componentDidMount() {
    const {source, loadUsers, loadRoles} = this.props

    loadUsers(source.links.users)
    if (source.links.roles) {
      loadRoles(source.links.roles)
    }
  }

  handleClickCreate(type) {
    if (this.props.isEditing) {
      this.props.addFlashMessage({type: 'error', text: `You can only add one ${type.slice(0, -1)} at a time`})
      return
    }
    this.props.setEditingMode(true)
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
      await this.props.createUser(this.props.source.links.users, user)

      // this.props.editUser(Object.assign(
      //   this.props.editingUser,
      //   {isEditing: undefined, isNew: undefined}))
      // this.props.createUser(urlUsers, this.props.editingUser, this.props.addFlashMessage)
    } else {
      // TODO update user
      // console.log('update')
    }
    this.props.setEditingMode(false)
  }

  handleCancelEdit(user) {
    this.props.removeUser(user)
    this.props.setEditingMode(false)
    // this.props.clearEditingMode()
    // this.props.editUser(null)
  }

  handleDeleteRole(role) {
    this.props.deleteRole(role, this.props.addFlashMessage)
  }

  handleDeleteUser(user) {
    this.props.deleteUser(user, this.props.addFlashMessage)
  }

  render() {
    const {users, roles, source, isEditing, filterUsers, filterRoles, addFlashMessage} = this.props

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
              <div className="col-md-12">
                {
                  users.length ?
                  <AdminTabs
                    users={users}
                    roles={roles}
                    source={source}
                    isEditing={isEditing}
                    onClickCreate={this.handleClickCreate}
                    onEditUser={this.handleEditUser}
                    onSaveUser={this.handleSaveUser}
                    onCancelEdit={this.handleCancelEdit}
                    onDeleteRole={this.handleDeleteRole}
                    onDeleteUser={this.handleDeleteUser}
                    onFilterUsers={filterUsers}
                    onFilterRoles={filterRoles}
                    addFlashMessage={addFlashMessage}
                  /> :
                  <span>Loading...</span>
                }
              </div>
            </div>
          </div>
        </div>
      </div>
    )
  }
}

const {
  arrayOf,
  bool,
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
  loadUsers: func,
  loadRoles: func,
  addUser: func,
  setEditingMode: func,
  removeUser: func,
  isEditing: bool,
  editUser: func,
  createUser: func,
  deleteRole: func,
  deleteUser: func,
  addFlashMessage: func,
  filterRoles: func,
  filterUsers: func,
}

const mapStateToProps = ({admin: {users, roles, ephemeral: {isEditing}}}) => ({
  users,
  roles,
  isEditing,
})

const mapDispatchToProps = (dispatch) => ({
  loadUsers: bindActionCreators(loadUsersAsync, dispatch),
  loadRoles: bindActionCreators(loadRolesAsync, dispatch),
  addUser: bindActionCreators(addUser, dispatch),
  setEditingMode: bindActionCreators(setEditingMode, dispatch),
  removeUser: bindActionCreators(deleteUser, dispatch),
  editUser: bindActionCreators(editUser, dispatch),
  createUser: bindActionCreators(createUserAsync, dispatch),
  deleteRole: bindActionCreators(deleteRoleAsync, dispatch),
  deleteUser: bindActionCreators(deleteUserAsync, dispatch),
  filterRoles: bindActionCreators(filterRolesAction, dispatch),
  filterUsers: bindActionCreators(filterUsersAction, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(AdminPage)
