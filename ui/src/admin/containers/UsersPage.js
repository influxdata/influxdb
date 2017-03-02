import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux';
import {bindActionCreators} from 'redux';
import {loadUsersAsync, loadRolesAsync} from 'src/admin/actions'
import UsersTabs from 'src/admin/components/UsersTabs'
import UsersTable from 'src/admin/components/UsersTable'

class UsersPage extends Component {
  constructor(props) {
    super(props)
  }

  componentDidMount() {
    const {source, loadUsers} = this.props
    loadUsers(source.links.users)
    if (source.links.roles) loadRoles(source.links.roles)
  }

  render() {
    const {users, roles} = this.props

    return (
      <div id="users-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">
              {roles.length ? <UsersTabs users={users} roles={roles} /> : <UsersTable users={users} />}
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

UsersPage.propTypes = {
  source: shape({
    id: string.isRequired,
    links: shape({
      users: string.isRequired,
    }),
  }).isRequired,
  users: arrayOf(shape()),
  roles: arrayOf(shape()),
  loadUsers: func,
}

const mapStateToProps = ({admin}) => ({
  users: admin.users,
  roles: admin.roles,
})

const mapDispatchToProps = (dispatch) => ({
  loadUsers: bindActionCreators(loadUsersAsync, dispatch),
  loadRoles: bindActionCreators(loadRolesAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(UsersPage);
