import React, {Component, PropTypes} from 'react'

import UserRow from 'src/admin/components/UserRow'
import EmptyRow from 'src/admin/components/EmptyRow'

const newDefaultUser = {
  name: '',
  password: '',
  roles: [],
  permissions: [],
}

class UsersTable extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isAddingUser: false,
      newUser: {...newDefaultUser},
    }

    this.handleCancelNewUser = ::this.handleCancelNewUser
    this.handleSubmitNewUser = ::this.handleSubmitNewUser
    this.handleEditNewUserName = ::this.handleEditNewUserName
    this.handleEditNewUserPassword = ::this.handleEditNewUserPassword
  }

  handleSubmitNewUser() {
    const {source, addUser, addFlashMessage} = this.props
    const {newUser} = this.state
    if (newUser.name.length >= 3 && newUser.password.length >= 3) {
      addUser(source.links.users, newUser)
    } else {
      addFlashMessage({type: 'error', text: `Username and password too short`})
    }
  }

  handleCancelNewUser() {
    this.setState({
      isAddingUser: false,
      newUser: {...newDefaultUser},
    })
  }

  handleEditNewUserName(val) {
    const newUser = Object.assign({}, this.state.newUser, {name: val})

    this.setState({newUser})
  }

  handleEditNewUserPassword(val) {
    const newUser = Object.assign({}, this.state.newUser, {password: val})

    this.setState({newUser})
  }

  render() {
    const {users} = this.props
    const {isAddingUser} = this.state

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
          <button
            className="btn btn-primary"
            onClick={() => this.setState({isAddingUser: true})}
          >Create User</button>
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
                isAddingUser ?
                  <UserRow
                    user={this.state.newUser}
                    isEditing={true}
                    onCancel={this.handleCancelNewUser}
                    onSave={this.handleSubmitNewUser}
                    onEditName={this.handleEditNewUserName}
                    onEditPassword={this.handleEditNewUserPassword}
                  />
                  : null
              }
              {
                users.length ?
                  users.map((user) =>
                    <UserRow
                      key={user.name}
                      user={user}
                    />
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
  func,
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
  source: shape(),
  addUser: func.isRequired,
  addFlashMessage: func.isRequired,
}

export default UsersTable
