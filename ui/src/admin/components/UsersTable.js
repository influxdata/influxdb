import React, {Component, PropTypes} from 'react'

import UserRow from 'src/admin/components/UserRow'
import EmptyRow from 'src/admin/components/EmptyRow'
import FilterBar from 'src/admin/components/FilterBar'

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

    this.handleClickCreate = ::this.handleClickCreate
    this.handleClearNewUser = ::this.handleClearNewUser
    this.handleSubmitNewUser = ::this.handleSubmitNewUser
    this.handleInputChange = ::this.handleInputChange
    this.handleInputKeyPress = ::this.handleInputKeyPress
  }

  handleClickCreate() {
    this.setState({isAddingUser: true})
  }

  handleSubmitNewUser() {
    const {onAdd, addFlashMessage} = this.props
    const {newUser} = this.state
    if (isValid(newUser)) {
      onAdd(newUser)
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
    const {users, hasRoles, onDelete, onFilter} = this.props
    const {isAddingUser} = this.state

    return (
      <div className="panel panel-info">
        <FilterBar name="Users" onFilter={onFilter} onClickCreate={this.handleClickCreate} />
        <div className="panel-body">
          <table className="table v-center admin-table">
            <thead>
              <tr>
                <th>User</th>
                {hasRoles && <th>Roles</th>}
                <th>Permissions</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {
                isAddingUser ?
                  <UserRow
                    user={this.state.newUser}
                    isEditing={true}
                    onDelete={this.handleClearNewUser}
                    onSave={this.handleSubmitNewUser}
                    onInputChange={this.handleInputChange}
                    onInputKeyPress={this.handleInputKeyPress}
                  />
                  : null
              }
              {
                users.length ?
                  users.filter(u => !u.hidden).map((user, i) =>
                    <UserRow key={i} user={user} onDelete={onDelete} />
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
  bool,
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
  onAdd: func.isRequired,
  addFlashMessage: func.isRequired,
  hasRoles: bool.isRequired,
  onDelete: func.isRequired,
  onFilter: func,
}

export default UsersTable
