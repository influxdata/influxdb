import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import * as adminChronografActionCreators from 'src/admin/actions/chronograf'
import {publishAutoDismissingNotification} from 'shared/dispatchers'

import {MEMBER_ROLE} from 'src/auth/Authorized'

import PageHeader from 'src/admin/components/chronograf/PageHeader'
import UsersTable from 'src/admin/components/chronograf/UsersTable'

import FancyScrollbar from 'shared/components/FancyScrollbar'

import {DEFAULT_ORG_ID} from 'src/admin/constants/dummyUsers'

class AdminChronografPage extends Component {
  // TODO: revisit this, possibly don't call setState if both are deep equal
  componentWillReceiveProps(nextProps) {
    const {currentOrganization} = nextProps

    const hasChangedCurrentOrganization =
      currentOrganization.id !== this.props.currentOrganization.id

    if (hasChangedCurrentOrganization) {
      this.loadUsers()
    }
  }

  componentDidMount() {
    this.loadUsers()
  }

  loadUsers = () => {
    const {links, actions: {loadUsersAsync}} = this.props

    loadUsersAsync(links.users)
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

  render() {
    const {users, currentOrganization} = this.props

    return (
      <div className="page">
        <PageHeader currentOrganization={currentOrganization} />
        <FancyScrollbar className="page-contents">
          {users
            ? <div className="container-fluid">
                <div className="row">
                  <div className="col-xs-12">
                    <UsersTable
                      users={users}
                      organization={currentOrganization}
                      onCreateUser={this.handleCreateUser}
                      onUpdateUserRole={this.handleUpdateUserRole()}
                      onUpdateUserSuperAdmin={this.handleUpdateUserSuperAdmin()}
                      onDeleteUser={this.handleDeleteUser}
                    />
                  </div>
                </div>
              </div>
            : <div className="page-spinner" />}
        </FancyScrollbar>
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

AdminChronografPage.propTypes = {
  links: shape({
    users: string.isRequired,
  }),
  users: arrayOf(shape),
  currentOrganization: shape({
    id: string.isRequired,
    name: string.isRequired,
  }).isRequired,
  actions: shape({
    loadUsersAsync: func.isRequired,
    createUserAsync: func.isRequired,
    updateUserAsync: func.isRequired,
    deleteUserAsync: func.isRequired,
  }),
  notify: func.isRequired,
}

const mapStateToProps = ({
  links,
  adminChronograf: {users},
  auth: {me: {currentOrganization}},
}) => ({
  links,
  users,
  currentOrganization,
})

const mapDispatchToProps = dispatch => ({
  actions: bindActionCreators(adminChronografActionCreators, dispatch),
  notify: bindActionCreators(publishAutoDismissingNotification, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(AdminChronografPage)
