import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux';
import {bindActionCreators} from 'redux';
import {loadUsersAsync, loadRolesAsync} from 'src/admin/actions'
import AdminTabs from 'src/admin/components/AdminTabs'

class AdminPage extends Component {
  constructor(props) {
    super(props)
  }

  componentDidMount() {
    const {source, loadUsers, loadRoles} = this.props

    loadUsers(source.links.users)
    if (source.links.roles) {
      loadRoles(source.links.roles)
    }
  }

  render() {
    const {users, roles, source} = this.props

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
                {users.length ? <AdminTabs users={users} roles={roles} source={source}/> : <span>Loading...</span>}
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
}

const mapStateToProps = ({admin}) => ({
  users: admin.users,
  roles: admin.roles,
})

const mapDispatchToProps = (dispatch) => ({
  loadUsers: bindActionCreators(loadUsersAsync, dispatch),
  loadRoles: bindActionCreators(loadRolesAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(AdminPage);
