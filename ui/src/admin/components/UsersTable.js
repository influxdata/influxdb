import React, {Component, PropTypes} from 'react'

import UserRow from 'src/admin/components/UserRow'
import EmptyRow from 'src/admin/components/EmptyRow'

const newDefaultUser = {
  name: '',
  password: '',
  roles: [],
  permissions: [],
}

const isValid = (user) => {
  const minLen = 3
  return (user.name.length >= minLen && user.password.length >= minLen)
}

class UsersTable extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isAddingUser: false,
      newUser: {...newDefaultUser},
    }

    this.handleClearNewUser = ::this.handleClearNewUser
    this.handleSubmitNewUser = ::this.handleSubmitNewUser
    this.handleInputChange = ::this.handleInputChange
    this.handleInputKeyPress = ::this.handleInputKeyPress
  }

  handleSubmitNewUser() {
    const {source, addUser, addFlashMessage} = this.props
    const {newUser} = this.state
    if (isValid(newUser)) {
      addUser(source.links.users, newUser)
      this.handleClearNewUser()
    } else {
      addFlashMessage({type: 'error', text: 'Username and/or password too short'})
    }
  }

  handleClearNewUser() {
    this.setState({
      isAddingUser: false,
      newUser: {...newDefaultUser},
    })
  }

  handleInputChange(e) {
    const newUser = Object.assign({}, this.state.newUser, {[e.target.name]: e.target.value})

    this.setState({newUser})
  }

  handleInputKeyPress(e) {
    if (e.key === 'Enter') {
      this.handleSubmitNewUser()
    }
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
                    onCancel={this.handleClearNewUser}
                    onSave={this.handleSubmitNewUser}
                    onInputChange={this.handleInputChange}
                    onInputKeyPress={this.handleInputKeyPress}
                  />
                  : null
              }
              {
                users.length ?
                  users.map((user, i) =>
                    <UserRow
                      key={i}
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
