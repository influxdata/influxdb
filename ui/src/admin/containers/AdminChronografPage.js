import React, {Component, PropTypes} from 'react'

import SourceIndicator from 'shared/components/SourceIndicator'
import UsersTable from 'src/admin/components/chronograf/UsersTable'
import BatchActionsBar from 'src/admin/components/chronograf/BatchActionsBar'
import CreateOrgOverlay from 'src/admin/components/chronograf/CreateOrgOverlay'
import Dropdown from 'shared/components/Dropdown'

import FancyScrollbar from 'shared/components/FancyScrollbar'

import {
  DUMMY_USERS,
  DUMMY_ORGS,
  DEFAULT_ORG,
  NO_ORG,
} from 'src/admin/constants/dummyUsers'

class AdminChronografPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      // TODO: pass around organization object instead of just name
      organizationName: DEFAULT_ORG,
      selectedUsers: [],
      filteredUsers: this.props.users,
      showCreateOverlay: false,
    }
  }

  isSameUser = (userA, userB) => {
    return (
      userA.name === userB.name &&
      userA.provider === userB.provider &&
      userA.scheme === userB.scheme
    )
  }

  handleFilterUsers = filterQuery => {
    const {users} = this.props
    const {name} = filterQuery

    const filteredUsers =
      name === DEFAULT_ORG
        ? users
        : users.filter(
            user =>
              name === NO_ORG
                ? user.roles.length === 1 // Filter out if user is only part of Default Org
                : user.roles.find(role => role.organizationName === name)
          )
    this.setState({
      filteredUsers,
      organizationName: name,
      selectedUsers:
        filterQuery.name === DEFAULT_ORG ? this.state.selectedUsers : [],
    })
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

  handleBatchDeleteUsers = () => {}
  handleBatchRemoveOrgFromUsers = () => {}
  handleBatchAddOrgToUsers = () => {}
  handleBatchChangeUsersRole = () => {}

  handleUpdateUserRole = () => (user, currentRole, newRole) => {
    console.log(user, currentRole, newRole)
  }

  handleShowCreateOrgOverlay = () => {
    this.setState({showCreateOverlay: true})
  }

  handleHideCreateOrgOverlay = () => {
    this.setState({showCreateOverlay: false})
  }

  handleCreateOrganization = orgName => {
    console.log(orgName)
  }

  render() {
    const {users, organizations} = this.props
    const {
      organizationName,
      selectedUsers,
      filteredUsers,
      showCreateOverlay,
    } = this.state
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
              <button
                className="btn btn-primary btn-sm"
                onClick={this.handleShowCreateOrgOverlay}
              >
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
                      <div className="panel-heading u-flex u-ai-center u-jc-space-between">
                        <div className="u-flex u-ai-center">
                          <p className="dropdown-label">Filter Users</p>
                          <Dropdown
                            items={organizations.map(org => ({
                              ...org,
                              text: org.name,
                            }))}
                            selected={organizationName}
                            onChoose={this.handleFilterUsers}
                            buttonSize="btn-md"
                            className="dropdown-220"
                          />
                        </div>
                        <div className="users__search-widget input-group">
                          <input
                            type="text"
                            className="form-control"
                            placeholder="Filter Users..."
                          />
                          <div className="input-group-addon">
                            <span className="icon search" />
                          </div>
                        </div>
                      </div>
                      <BatchActionsBar
                        numUsersSelected={numUsersSelected}
                        organizationName={organizationName}
                        organizations={organizations}
                        onDeleteUsers={this.handleBatchDeleteUsers}
                        onAddOrgs={this.handleBatchAddOrgToUsers}
                        onRemoveOrgs={this.handleBatchRemoveOrgFromUsers}
                        onChangeRoles={this.handleBatchChangeUsersRole}
                      />
                      <div className="panel-body chronograf-admin-table--panel">
                        <UsersTable
                          filteredUsers={filteredUsers}
                          organizationName={organizationName}
                          onFilterUsers={this.handleFilterUsers}
                          onToggleUserSelected={this.handleToggleUserSelected}
                          selectedUsers={selectedUsers}
                          isSameUser={this.isSameUser}
                          onToggleAllUsersSelected={
                            this.handleToggleAllUsersSelected
                          }
                          onUpdateUserRole={this.handleUpdateUserRole()}
                        />
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            : <div className="page-spinner" />}
        </FancyScrollbar>
        {showCreateOverlay
          ? <CreateOrgOverlay
              onDismiss={this.handleHideCreateOrgOverlay}
              onCreateOrg={this.handleCreateOrganization}
            />
          : null}
      </div>
    )
  }
}

const {arrayOf, shape} = PropTypes

AdminChronografPage.propTypes = {
  users: arrayOf(shape),
  organizations: arrayOf(shape),
}

AdminChronografPage.defaultProps = {
  users: DUMMY_USERS,
  organizations: DUMMY_ORGS,
}

export default AdminChronografPage
