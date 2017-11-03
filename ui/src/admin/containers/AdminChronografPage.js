import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import * as adminChronografActionCreators from 'src/admin/actions/chronograf'
import {publishAutoDismissingNotification} from 'shared/dispatchers'

import Authorized, {SUPERADMIN_ROLE} from 'src/auth/Authorized'

import PageHeader from 'src/admin/components/chronograf/PageHeader'
import UsersTableHeader from 'src/admin/components/chronograf/UsersTableHeader'
import UsersTable from 'src/admin/components/chronograf/UsersTable'
import BatchActionsBar from 'src/admin/components/chronograf/BatchActionsBar'
import ManageOrgsOverlay from 'src/admin/components/chronograf/ManageOrgsOverlay'
import CreateUserOverlay from 'src/admin/components/chronograf/CreateUserOverlay'

import FancyScrollbar from 'shared/components/FancyScrollbar'

import {isSameUser} from 'shared/reducers/helpers/auth'

import {
  DEFAULT_ORG_NAME,
  NO_ORG,
  USER_ROLES,
} from 'src/admin/constants/dummyUsers'

class AdminChronografPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      // TODO: pass around organization object instead of just name
      organizationName: this.props.currentOrganization.name,
      selectedUsers: [],
      filteredUsers: this.props.users,
      showManageOverlay: false,
      showCreateUserOverlay: false,
    }
  }

  // TODO: revisit this, possibly don't call setState if both are deep equal
  componentWillReceiveProps(nextProps) {
    const {users, currentOrganization} = nextProps

    this.handleFilterUsers({
      name: currentOrganization.name,
      users,
    })
  }

  componentDidMount() {
    const {
      links,
      actions: {loadUsersAsync, loadOrganizationsAsync},
    } = this.props

    loadUsersAsync(links.users)
    loadOrganizationsAsync(links.organizations) // TODO: make sure server allows admin to hit this for safety
  }

  handleFilterUsers = ({users, name}) => {
    const nextUsers = users || this.props.users
    const nextOrganizationName = name || this.props.currentOrganization.name

    const filteredUsers =
      nextOrganizationName === DEFAULT_ORG_NAME
        ? nextUsers
        : nextUsers.filter(
            user =>
              nextOrganizationName === NO_ORG
                ? user.roles.length === 1 // Filter out if user is only part of Default Org
                : user.roles.find(
                    role => role.organizationName === nextOrganizationName
                  )
          )
    this.setState({
      filteredUsers,
      organizationName: nextOrganizationName,
      selectedUsers:
        nextOrganizationName === DEFAULT_ORG_NAME
          ? this.state.selectedUsers
          : [],
    })
  }

  handleToggleUserSelected = user => e => {
    e.preventDefault()

    const {selectedUsers} = this.state

    const isUserSelected = selectedUsers.find(u => isSameUser(user, u))

    const newSelectedUsers = isUserSelected
      ? selectedUsers.filter(u => !isSameUser(user, u))
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

  // TODO: make this work for batch. was made to work for a single user since
  // there's not currently UI to delete a single user
  handleBatchDeleteUsers = () => {
    const {actions: {deleteUserAsync}, notify} = this.props
    const {selectedUsers} = this.state

    if (selectedUsers.length > 1) {
      notify(
        'error',
        'Batch actions for more than 1 user not currently supported'
      )
      return
    }

    deleteUserAsync(selectedUsers[0])
  }

  handleBatchRemoveOrgFromUsers = () => {}
  handleBatchAddOrgToUsers = () => {}
  handleBatchChangeUsersRole = () => {}

  handleUpdateUserRole = () => (_user, _currentRole, _newRole) => {}

  handleShowManageOrgsOverlay = () => {
    this.setState({showManageOverlay: true})
  }

  handleShowCreateUserOverlay = () => {
    this.setState({showCreateUserOverlay: true})
  }

  handleHideOverlays = () => {
    this.setState({showManageOverlay: false, showCreateUserOverlay: false})
  }

  handleCreateOrganization = organizationName => {
    const {links, actions: {createOrganizationAsync}} = this.props

    createOrganizationAsync(links.organizations, {name: organizationName})
  }

  handleDeleteOrganization = _organization => {}
  handleRenameOrganization = (_organization, _newOrgName) => {}

  handleCreateUser = user => {
    const {links, actions: {createUserAsync}} = this.props

    createUserAsync(links.users, user)
  }

  render() {
    const {users, organizations} = this.props
    const {
      organizationName,
      selectedUsers,
      filteredUsers,
      showManageOverlay,
      showCreateUserOverlay,
    } = this.state

    const numUsersSelected = Object.keys(selectedUsers).length

    return (
      <div className="page">
        <PageHeader
          onShowManageOrgsOverlay={this.handleShowManageOrgsOverlay}
          onShowCreateUserOverlay={this.handleShowCreateUserOverlay}
        />

        <FancyScrollbar className="page-contents">
          {users
            ? <div className="container-fluid">
                <div className="row">
                  <div className="col-xs-12">
                    <div className="panel panel-minimal">
                      <UsersTableHeader
                        organizationName={organizationName}
                        organizations={organizations}
                        onFilterUsers={this.handleFilterUsers}
                      />
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
                        <Authorized
                          requiredRole={SUPERADMIN_ROLE}
                          propsOverride={{organizationName}}
                        >
                          <UsersTable
                            filteredUsers={filteredUsers} // TODO: change to users upon separating Orgs & Users views
                            organizationName={organizationName}
                            organizations={organizations}
                            onFilterUsers={this.handleFilterUsers}
                            onToggleUserSelected={this.handleToggleUserSelected}
                            selectedUsers={selectedUsers}
                            isSameUser={isSameUser}
                            onToggleAllUsersSelected={
                              this.handleToggleAllUsersSelected
                            }
                            onUpdateUserRole={this.handleUpdateUserRole()}
                          />
                        </Authorized>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            : <div className="page-spinner" />}
        </FancyScrollbar>
        {showManageOverlay
          ? <ManageOrgsOverlay
              onDismiss={this.handleHideOverlays}
              onCreateOrg={this.handleCreateOrganization}
              onDeleteOrg={this.handleDeleteOrganization}
              onRenameOrg={this.handleRenameOrganization}
              organizations={organizations}
            />
          : null}
        {showCreateUserOverlay
          ? <CreateUserOverlay
              onDismiss={this.handleHideOverlays}
              onCreateUser={this.handleCreateUser}
              userRoles={USER_ROLES}
              organizations={organizations}
            />
          : null}
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

AdminChronografPage.propTypes = {
  links: shape({
    users: string.isRequired,
    organizations: string.isRequired,
  }),
  users: arrayOf(shape),
  organizations: arrayOf(
    shape({
      id: string.isRequired,
      name: string.isRequired,
    })
  ),
  currentOrganization: shape({
    id: string.isRequired,
    name: string.isRequired,
  }).isRequired,
  actions: shape({
    loadUsersAsync: func.isRequired,
    loadOrganizationsAsync: func.isRequired,
    createUserAsync: func.isRequired,
    deleteUserAsync: func.isRequired,
    createOrganizationAsync: func.isRequired,
  }),
  notify: func.isRequired,
}

const mapStateToProps = ({
  links,
  adminChronograf: {users, organizations},
  auth: {me: {currentOrganization}},
}) => ({
  links,
  users,
  organizations,
  currentOrganization,
})

const mapDispatchToProps = dispatch => ({
  actions: bindActionCreators(adminChronografActionCreators, dispatch),
  notify: bindActionCreators(publishAutoDismissingNotification, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(AdminChronografPage)
