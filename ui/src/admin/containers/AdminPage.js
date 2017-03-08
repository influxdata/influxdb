import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {
  loadUsersAsync,
  loadRolesAsync,
  addUser,
  updateEditingUser,
  clearEditingMode,
  removeAddedUser,
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
    if (type === 'users') {
      this.props.addUser()
    }
  }

  handleEditUser(user) {
    this.props.updateEditingUser(user)
  }

  handleSaveUser() {
    if (!isValid(this.props.editingUser)) {
      this.props.addFlashMessage({type: 'error', text: 'Username and/or password too short'})
      return
    }
    if (this.props.editingUser.isNew) {
      const urlUsers = this.props.source.links.users
      const userLink = `${urlUsers}/${this.props.editingUser.name}`

      this.props.updateEditingUser(Object.assign(
        this.props.editingUser,
        {links: {self: userLink}, isEditing: undefined, isNew: undefined}))
      this.props.createUser(urlUsers, this.props.editingUser, this.props.addFlashMessage)
      .then(() => this.props.clearEditingMode())
    } else {
      // TODO update user
      // console.log('update')
    }
  }

  handleCancelEdit() {
    this.props.clearEditingMode()
    this.props.removeAddedUser()
    this.props.updateEditingUser(null)
  }

  handleDeleteRole(role) {
    this.props.deleteRole(role, this.props.addFlashMessage)
  }

  handleDeleteUser(user) {
    this.props.deleteUser(user, this.props.addFlashMessage)
  }

  render() {
    const {users, roles, source, filterUsers, filterRoles, addFlashMessage} = this.props

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
  isEditing: bool,
  editingUser: shape(),
  updateEditingUser: func,
  clearEditingMode: func,
  removeAddedUser: func,
  createUser: func,
  deleteRole: func,
  deleteUser: func,
  addFlashMessage: func,
  filterRoles: func,
  filterUsers: func,
}

const mapStateToProps = ({admin: {users, roles, ephemeral: {isEditing, editingUser}}}) => ({
  users,
  roles,
  isEditing,
  editingUser,
})

const mapDispatchToProps = (dispatch) => ({
  loadUsers: bindActionCreators(loadUsersAsync, dispatch),
  loadRoles: bindActionCreators(loadRolesAsync, dispatch),
  addUser: bindActionCreators(addUser, dispatch),
  updateEditingUser: bindActionCreators(updateEditingUser, dispatch),
  clearEditingMode: bindActionCreators(clearEditingMode, dispatch),
  removeAddedUser: bindActionCreators(removeAddedUser, dispatch),
  createUser: bindActionCreators(createUserAsync, dispatch),
  deleteRole: bindActionCreators(deleteRoleAsync, dispatch),
  deleteUser: bindActionCreators(deleteUserAsync, dispatch),
  filterRoles: bindActionCreators(filterRolesAction, dispatch),
  filterUsers: bindActionCreators(filterUsersAction, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(AdminPage)
