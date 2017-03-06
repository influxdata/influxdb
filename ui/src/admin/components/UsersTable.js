import React, {Component, PropTypes} from 'react'
import UserRow from 'src/admin/components/UserRow'
import EmptyRow from 'src/admin/components/EmptyRow'

class UsersTable extends Component {
  constructor(props) {
    super(props)

    this.state = {users: this.props.users}

    this.addUser = ::this.addUser
  }

  addUser() {
    const newUser = {
      name: '',
      roles: [],
      permissions: [],
      isEditing: true,
    }

    const newUsers = [...this.state.users]
    newUsers.unshift(newUser)

    this.setState({users: newUsers})
  }

  render() {
    return (
      <div className="panel panel-info">
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <div className="users__search-widget input-group admin__search-widget">
            <input
              type="text"
              className="form-control"
              placeholder="Filter Role..."
            />
            <div className="input-group-addon">
              <span className="icon search" aria-hidden="true"></span>
            </div>
          </div>
          <button className="btn btn-primary" onClick={this.addUser}>Create User</button>
        </div>
        <div className="panel-body">
          <table className="table v-center">
            <thead>
              <tr>
                <th>User</th>
                <th>Roles</th>
                <th>Permissions</th>
              </tr>
            </thead>
            <tbody>
              {
                this.state.users.length ?
                  this.state.users.map((user) =>
                    <UserRow key={user.name} user={user} isEditing={user.isEditing} />
                  ) : <EmptyRow tableName={'Users'} />
              }
            </tbody>
          </table>
        </div>
      </div>
    )
  }
}

const {
  arrayOf,
  shape,
  string,
} = PropTypes

UsersTable.propTypes = {
  users: arrayOf(shape({
    name: string.isRequired,
    roles: arrayOf(shape({
      name: string,
    })),
    permissions: arrayOf(shape({
      name: string,
      scope: string.isRequired,
    })),
  })),
}

export default UsersTable
