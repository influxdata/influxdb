import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import * as adminChronografActionCreators from 'src/admin/actions/chronograf'
import * as configActionCreators from 'src/shared/actions/config'
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {ErrorHandling} from 'src/shared/decorators/errors'

import AllUsersTable from 'src/admin/components/chronograf/AllUsersTable'
import {
  AuthLinks,
  Organization,
  Role,
  User,
  Notification,
  NotificationFunc,
} from 'src/types'

interface Props {
  notify: (message: Notification | NotificationFunc) => void
  links: AuthLinks
  meID: string
  users: User[]
  organizations: Organization[]
  actionsAdmin: {
    loadUsersAsync: (link: string) => void
    loadOrganizationsAsync: (link: string) => void
    createUserAsync: (link: string, user: User) => void
    updateUserAsync: (user: User, updatedUser: User, message: string) => void
    deleteUserAsync: (
      user: User,
      deleteObj: {isAbsoluteDelete: boolean}
    ) => void
  }
  actionsConfig: {
    getAuthConfigAsync: (link: string) => void
    updateAuthConfigAsync: () => void
  }
  authConfig: {
    superAdminNewUsers: boolean
  }
}

interface State {
  isLoading: boolean
}

@ErrorHandling
export class AllUsersPage extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      isLoading: true,
    }
  }

  public componentDidMount() {
    const {
      links,
      actionsConfig: {getAuthConfigAsync},
    } = this.props
    getAuthConfigAsync(links.config.auth)
  }

  public async componentWillMount() {
    const {
      links,
      actionsAdmin: {loadOrganizationsAsync, loadUsersAsync},
    } = this.props

    this.setState({isLoading: true})

    await Promise.all([
      loadOrganizationsAsync(links.organizations),
      loadUsersAsync(links.allUsers),
    ])

    this.setState({isLoading: false})
  }

  public handleCreateUser = (user: User) => {
    const {
      links,
      actionsAdmin: {createUserAsync},
    } = this.props
    createUserAsync(links.allUsers, user)
  }

  public handleUpdateUserRoles = (
    user: User,
    roles: Role[],
    successMessage: string
  ) => {
    const {
      actionsAdmin: {updateUserAsync},
    } = this.props
    const updatedUser = {...user, roles}
    updateUserAsync(user, updatedUser, successMessage)
  }

  public handleUpdateUserSuperAdmin = (user: User, superAdmin: boolean) => {
    const {
      actionsAdmin: {updateUserAsync},
    } = this.props
    const updatedUser = {...user, superAdmin}
    updateUserAsync(
      user,
      updatedUser,
      `${user.name}'s SuperAdmin status has been updated`
    )
  }

  public handleDeleteUser = (user: User) => {
    const {
      actionsAdmin: {deleteUserAsync},
    } = this.props
    deleteUserAsync(user, {isAbsoluteDelete: true})
  }

  public render() {
    const {
      meID,
      users,
      links,
      notify,
      authConfig,
      actionsConfig,
      organizations,
    } = this.props

    return (
      <AllUsersTable
        meID={meID}
        users={users}
        links={links}
        notify={notify}
        authConfig={authConfig}
        actionsConfig={actionsConfig}
        organizations={organizations}
        isLoading={this.state.isLoading}
        onDeleteUser={this.handleDeleteUser}
        onCreateUser={this.handleCreateUser}
        onUpdateUserRoles={this.handleUpdateUserRoles}
        onUpdateUserSuperAdmin={this.handleUpdateUserSuperAdmin}
      />
    )
  }
}

const mapStateToProps = ({
  links,
  adminChronograf: {organizations, users},
  config: {auth: authConfig},
}) => ({
  authConfig,
  links,
  organizations,
  users,
})

const mapDispatchToProps = dispatch => ({
  actionsAdmin: bindActionCreators(adminChronografActionCreators, dispatch),
  actionsConfig: bindActionCreators(configActionCreators, dispatch),
  notify: bindActionCreators(notifyAction, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(AllUsersPage)
