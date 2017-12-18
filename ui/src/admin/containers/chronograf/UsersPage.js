import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import * as adminChronografActionCreators from 'src/admin/actions/chronograf'
import {publishAutoDismissingNotification} from 'shared/dispatchers'

import UsersTable from 'src/admin/components/chronograf/UsersTable'

class UsersPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isLoading: true,
    }
  }

  // // TODO: revisit this, possibly don't call setState if both are deep equal
  // componentWillReceiveProps(nextProps) {
  //   const {meCurrentOrganization} = nextProps
  //
  //   const hasChangedCurrentOrganization =
  //     meCurrentOrganization.id !== this.props.meCurrentOrganization.id
  //
  //   if (hasChangedCurrentOrganization) {
  //     this.setState({isLoading: true})
  //     this.loadUsers()
  //   }
  // }

  // loadUsers = () => {
  //   // this.setState({isLoading: true})
  //   return loadUsersAsync(links.users)
  // }

  handleCreateUser = user => {
    const {links, actions: {createUserAsync}} = this.props
    createUserAsync(links.users, user)
  }

  handleUpdateUserRole = (user, currentRole, {name}) => {
    const {actions: {updateUserAsync}} = this.props
    const updatedRole = {...currentRole, name}
    const newRoles = user.roles.map(
      r => (r.organization === currentRole.organization ? updatedRole : r)
    )
    updateUserAsync(user, {...user, roles: newRoles})
  }

  handleUpdateUserSuperAdmin = (user, superAdmin) => {
    const {actions: {updateUserAsync}} = this.props
    const updatedUser = {...user, superAdmin}
    updateUserAsync(user, updatedUser)
  }

  handleDeleteUser = user => {
    const {actions: {deleteUserAsync}} = this.props
    deleteUserAsync(user)
  }

  async componentWillMount() {
    const {
      links,
      actions: {loadOrganizationsAsync, loadUsersAsync},
    } = this.props

    this.setState({isLoading: true})

    loadOrganizationsAsync(links.organizations)
    await loadUsersAsync(links.users)

    this.setState({isLoading: false})
  }

  render() {
    const {
      meCurrentOrganization,
      organizations,
      meID,
      users,
      notify,
    } = this.props
    const {isLoading} = this.state

    if (isLoading || !(organizations.length && users.length)) {
      return <div className="generic-empty-state">Loading...</div>
    }

    const organization = organizations.find(
      o => o.id === meCurrentOrganization.id
    )

    return (
      <UsersTable
        meID={meID}
        users={users}
        organization={organization}
        onCreateUser={this.handleCreateUser}
        onUpdateUserRole={this.handleUpdateUserRole}
        onUpdateUserSuperAdmin={this.handleUpdateUserSuperAdmin}
        onDeleteUser={this.handleDeleteUser}
        notify={notify}
      />
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

UsersPage.propTypes = {
  links: shape({
    users: string.isRequired,
  }),
  meID: string.isRequired,
  meCurrentOrganization: shape({
    id: string.isRequired,
    name: string.isRequired,
  }).isRequired,
  users: arrayOf(shape),
  organizations: arrayOf(shape),
  actions: shape({
    loadUsersAsync: func.isRequired,
    loadOrganizationsAsync: func.isRequired,
    createUserAsync: func.isRequired,
    updateUserAsync: func.isRequired,
    deleteUserAsync: func.isRequired,
  }),
  notify: func.isRequired,
}

const mapStateToProps = ({links, adminChronograf: {organizations, users}}) => ({
  links,
  organizations,
  users,
})

const mapDispatchToProps = dispatch => ({
  actions: bindActionCreators(adminChronografActionCreators, dispatch),
  notify: bindActionCreators(publishAutoDismissingNotification, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(UsersPage)
