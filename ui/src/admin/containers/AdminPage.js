import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {
  loadUsersAsync,
  loadRolesAsync,
  deleteRoleAsync,
  deleteUserAsync,
  filterRoles as filterRolesAction,
  filterUsers as filterUsersAction,
} from 'src/admin/actions'
import AdminTabs from 'src/admin/components/AdminTabs'

class AdminPage extends Component {
  constructor(props) {
    super(props)
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

  handleDeleteRole(role) {
    this.props.deleteRole(role, this.props.addFlashMessage)
  }

  handleDeleteUser(user) {
    this.props.deleteUser(user, this.props.addFlashMessage)
  }

  render() {
    const {users, roles, source, filterUsers, filterRoles} = this.props

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
                    onDeleteRole={this.handleDeleteRole}
                    onDeleteUser={this.handleDeleteUser}
                    onFilterUsers={filterUsers}
                    onFilterRoles={filterRoles}
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
  deleteRole: func,
  deleteUser: func,
  addFlashMessage: func,
  filterRoles: func,
  filterUsers: func,
}

const mapStateToProps = ({admin}) => ({
  users: admin.users,
  roles: admin.roles,
})

const mapDispatchToProps = (dispatch) => ({
  loadUsers: bindActionCreators(loadUsersAsync, dispatch),
  loadRoles: bindActionCreators(loadRolesAsync, dispatch),
  deleteRole: bindActionCreators(deleteRoleAsync, dispatch),
  deleteUser: bindActionCreators(deleteUserAsync, dispatch),
  filterRoles: bindActionCreators(filterRolesAction, dispatch),
  filterUsers: bindActionCreators(filterUsersAction, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(AdminPage)
