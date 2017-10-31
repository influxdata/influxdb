import React, {Component, PropTypes} from 'react'

import SourceIndicator from 'shared/components/SourceIndicator'
import AllUsersTable from 'src/admin/components/chronograf/AllUsersTable'

import FancyScrollbar from 'shared/components/FancyScrollbar'

import {DUMMY_USERS} from 'src/admin/constants/dummyUsers'

class AdminChronografPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      organizationName: null,
      selectedUsers: [],
      filteredUsers: this.props.users,
    }
  }

  isSameUser = (userA, userB) => {
    return (
      userA.name === userB.name &&
      userA.provider === userB.provider &&
      userA.scheme === userB.scheme
    )
  }

  handleFilterUsers = organizationName => () => {
    const {users} = this.props

    const filteredUsers = organizationName
      ? users.filter(user => {
          return user.roles.find(
            role => role.organizationName === organizationName
          )
        })
      : users
    this.setState({filteredUsers})
  }

  handleToggleUserSelected = user => e => {
    e.preventDefault()

    const {selectedUsers} = this.state

    const isUserSelected = selectedUsers.find(u => this.isSameUser(user, u))

    const newSelectedUsers = isUserSelected
      ? selectedUsers.filter(u => !this.isSameUser(user, u))
      : [...selectedUsers, user]

    this.setState({selectedUsers: newSelectedUsers})
  }

  handleToggleAllUsersSelected = areAllSelected => () => {
    const {filteredUsers} = this.state

    if (areAllSelected) {
      this.setState({selectedUsers: []})
    } else {
      this.setState({selectedUsers: filteredUsers})
    }
  }

  render() {
    const {users} = this.props
    const {organizationName, selectedUsers, filteredUsers} = this.state
    const numUsersSelected = Object.keys(selectedUsers).length
    return (
      <div className="page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1 className="page-header__title">Chronograf Admin</h1>
            </div>
            <div className="page-header__right">
              <SourceIndicator />
              <button className="btn btn-primary btn-sm">
                <span className="icon plus" />
                Create Organization
              </button>
            </div>
          </div>
        </div>
        <FancyScrollbar className="page-contents">
          {users
            ? <div className="container-fluid">
                <div className="row">
                  <div className="col-xs-12">
                    <div className="panel panel-minimal">
                      {numUsersSelected
                        ? <div className="panel-heading">
                            <h2 className="panel-title">
                              {numUsersSelected} User{numUsersSelected > 1 ? 's' : ''}{' '}
                              Selected
                            </h2>
                          </div>
                        : <div className="panel-heading">
                            <h2 className="panel-title">
                              {organizationName
                                ? `Users in ${organizationName}`
                                : 'Users'}
                            </h2>
                          </div>}
                      <div className="panel-body">
                        <AllUsersTable
                          filteredUsers={filteredUsers}
                          organizationName={organizationName}
                          onFilterUsers={this.handleFilterUsers}
                          onToggleUserSelected={this.handleToggleUserSelected}
                          selectedUsers={selectedUsers}
                          isSameUser={this.isSameUser}
                          onToggleAllUsersSelected={
                            this.handleToggleAllUsersSelected
                          }
                        />
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            : <div className="page-spinner" />}
        </FancyScrollbar>
      </div>
    )
  }
}

const {arrayOf, shape} = PropTypes

AdminChronografPage.propTypes = {
  users: arrayOf(shape()),
}

AdminChronografPage.defaultProps = {
  users: DUMMY_USERS,
}

export default AdminChronografPage
