import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import * as adminChronografActionCreators from 'src/admin/actions/chronograf'
import {publishAutoDismissingNotification} from 'shared/dispatchers'

import {MEMBER_ROLE} from 'src/auth/Authorized'

import PageHeader from 'src/admin/components/chronograf/PageHeader'
import UsersTable from 'src/admin/components/chronograf/UsersTable'
import ManageOrgsOverlay from 'src/admin/components/chronograf/ManageOrgsOverlay'

import FancyScrollbar from 'shared/components/FancyScrollbar'

import {isSameUser} from 'shared/reducers/helpers/auth'

import {
  DEFAULT_ORG_ID,
  DEFAULT_ORG_NAME,
  NO_ORG,
} from 'src/admin/constants/dummyUsers'

class AdminChronografPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      selectedUsers: [],
      showManageOverlay: false,
    }
  }

  // TODO: revisit this, possibly don't call setState if both are deep equal
  componentWillReceiveProps(nextProps) {
    const {users, currentOrganization} = nextProps
    const {links} = this.props

    const hasChangedCurrentOrganization =
      currentOrganization.id !== this.props.currentOrganization.id

    if (hasChangedCurrentOrganization) {
      this.loadUsers()
    }
  }

  componentDidMount() {
    this.loadUsers()
    this.loadOrgs()
  }

  handleShowManageOrgsOverlay = () => {
    this.setState({showManageOverlay: true})
  }

  handleHideOverlays = () => {
    this.setState({showManageOverlay: false})
  }

  loadUsers = () => {
    const {links, actions: {loadUsersAsync}} = this.props

    loadUsersAsync(links.users)
  }

  loadOrgs = () => {
    const {links, actions: {loadOrganizationsAsync}} = this.props

    loadOrganizationsAsync(links.organizations) // TODO: make sure server allows admin to hit this for safety
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
    const {users} = this.props

    if (areAllSelected) {
      this.setState({selectedUsers: []})
    } else {
      this.setState({selectedUsers: users})
    }
  }

  // SINGLE USER ACTIONS
  handleCreateUser = user => {
    const {links, actions: {createUserAsync}} = this.props
    let newUser = user

    if (
      user.roles.length === 1 &&
      user.roles[0].organization !== DEFAULT_ORG_ID
    ) {
      newUser = {
        ...newUser,
        roles: [
          ...newUser.roles,
          {organization: DEFAULT_ORG_ID, name: MEMBER_ROLE},
        ],
      }
    }
    createUserAsync(links.users, newUser)
  }
  // handleAddUserToOrg will add a user to an organization as a 'member'. if
  // the user already has a role in that organization, it will do nothing.
  handleAddUserToOrg = (user, organization) => {
    const {actions: {updateUserAsync}} = this.props

    updateUserAsync(user, {
      ...user,
      roles: [
        ...user.roles,
        {
          name: MEMBER_ROLE, // TODO: remove this to let server decide when default org role is implemented
          organization: organization.id,
        },
      ],
    })
  }
  handleRemoveUserFromOrg = (user, organization) => {
    const {actions: {updateUserAsync}} = this.props

    let newRoles = user.roles.filter(r => r.organization !== organization.id)
    if (newRoles.length === 0) {
      newRoles = [{organization: DEFAULT_ORG_ID, name: MEMBER_ROLE}]
    }

    updateUserAsync(user, {...user, roles: newRoles})
  }
  handleUpdateUserRole = () => (user, currentRole, {name}) => {
    const {actions: {updateUserAsync}} = this.props

    const updatedRole = {...currentRole, name}
    const newRoles = user.roles.map(
      r => (r.organization === currentRole.organization ? updatedRole : r)
    )

    updateUserAsync(user, {...user, roles: newRoles})
  }
  handleUpdateUserSuperAdmin = () => (user, currentStatus, {value}) => {
    const {actions: {updateUserAsync}} = this.props

    const updatedUser = {...user, superAdmin: value}

    updateUserAsync(user, updatedUser)
  }
  handleDeleteUser = user => {
    const {actions: {deleteUserAsync}} = this.props
    deleteUserAsync(user)
  }

  // BATCH USER ACTIONS
  // TODO: make batch actions work for batch. currently only work for one user
  // since batch actions have not been implemented in the API.
  handleBatchChangeUsersRole = () => {}

  handleBatchDeleteUsers = () => {
    const {notify} = this.props
    // const {selectedUsers} = this.state

    if (this.state.selectedUsers.length > 1) {
      notify(
        'error',
        'Batch actions for more than 1 user not currently supported'
      )
    } else {
      this.handleDeleteUser(this.state.selectedUsers[0])
      console.log('reset selectedUsers')
      this.setState({selectedUsers: []})
    }
  }

  // SINGLE ORGANIZATION ACTIONS
  handleCreateOrganization = organizationName => {
    const {links, actions: {createOrganizationAsync}} = this.props
    createOrganizationAsync(links.organizations, {name: organizationName})
  }
  handleRenameOrganization = (organization, name) => {
    const {actions: {renameOrganizationAsync}} = this.props
    renameOrganizationAsync(organization, {...organization, name})
  }
  handleDeleteOrganization = organization => {
    const {actions: {deleteOrganizationAsync}} = this.props
    deleteOrganizationAsync(organization)
  }

  render() {
    const {users, organizations, currentOrganization} = this.props
    const {selectedUsers, showManageOverlay} = this.state

    console.log('currentOrg:', currentOrganization)
    console.log('Users:', users)

    return (
      <div className="page">
        <PageHeader
          onShowManageOrgsOverlay={this.handleShowManageOrgsOverlay}
          currentOrganization={currentOrganization}
        />
        <FancyScrollbar className="page-contents">
          {users
            ? <div className="container-fluid">
                <div className="row">
                  <div className="col-xs-12">
                    <UsersTable
                      users={users}
                      selectedUsers={selectedUsers}
                      onToggleUserSelected={this.handleToggleUserSelected}
                      onToggleAllUsersSelected={
                        this.handleToggleAllUsersSelected
                      }
                      isSameUser={isSameUser}
                      organization={currentOrganization}
                      onUpdateUserRole={this.handleUpdateUserRole()}
                      onCreateUser={this.handleCreateUser}
                      onUpdateUserSuperAdmin={this.handleUpdateUserSuperAdmin()}
                      onDeleteUsers={this.handleBatchDeleteUsers}
                      onChangeRoles={this.handleBatchChangeUsersRole}
                    />
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
      id: string, // when optimistically created, it will not have an id
      name: string.isRequired,
      link: string,
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
    updateUserAsync: func.isRequired,
    deleteUserAsync: func.isRequired,
    createOrganizationAsync: func.isRequired,
    renameOrganizationAsync: func.isRequired,
    deleteOrganizationAsync: func.isRequired,
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
