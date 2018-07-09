import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {
  addUser,
  addRole,
  editUser,
  editRole,
  deleteUser,
  deleteRole,
  loadUsersAsync,
  loadRolesAsync,
  createUserAsync,
  createRoleAsync,
  deleteUserAsync,
  deleteRoleAsync,
  loadPermissionsAsync,
  updateRoleUsersAsync,
  updateUserRolesAsync,
  updateUserPasswordAsync,
  updateRolePermissionsAsync,
  updateUserPermissionsAsync,
  filterUsers as filterUsersAction,
  filterRoles as filterRolesAction,
} from 'src/admin/actions/influxdb'

import UsersTable from 'src/admin/components/UsersTable'
import RolesTable from 'src/admin/components/RolesTable'
import QueriesPage from 'src/admin/containers/QueriesPage'
import DatabaseManagerPage from 'src/admin/containers/DatabaseManagerPage'
import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import SubSections from 'src/shared/components/SubSections'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {notify as notifyAction} from 'src/shared/actions/notifications'
import {
  Source,
  User as InfluxDBUser,
  Role as InfluxDBRole,
  Permission,
  RemoteDataState,
  SourceAuthenticationMethod,
} from 'src/types'
import {InfluxDBPermissions} from 'src/types/auth'
import {NotificationAction} from 'src/types/notifications'

import {
  notifyRoleNameInvalid,
  notifyDBUserNamePasswordInvalid,
} from 'src/shared/copy/notifications'

const isValidUser = user => {
  const minLen = 3
  return user.name.length >= minLen && user.password.length >= minLen
}

const isValidRole = role => {
  const minLen = 3
  return role.name.length >= minLen
}

interface User extends InfluxDBUser {
  isEditing: boolean
}

interface Role extends InfluxDBRole {
  isEditing: boolean
}

interface Props {
  source: Source
  users: User[]
  roles: Role[]
  permissions: Permission[]
  loadUsers: (url: string) => void
  loadRoles: (url: string) => void
  loadPermissions: (url: string) => void
  addUser: () => void
  addRole: () => void
  removeUser: (user: User) => void
  removeRole: (role: Role) => void
  editUser: (user: User, updates: Partial<User>) => void
  editRole: (role: Role, updates: Partial<Role>) => void
  createUser: (url: string, user: User) => void
  createRole: (url: string, role: Role) => void
  deleteRole: (role: Role) => void
  deleteUser: (user: User) => void
  filterRoles: () => void
  filterUsers: () => void
  updateRoleUsers: (role: Role, users: User[]) => void
  updateRolePermissions: (role: Role, permissions: Permission[]) => void
  updateUserPermissions: (user: User, permissions: Permission[]) => void
  updateUserRoles: (user: User, roles: Role[]) => void
  updateUserPassword: (user: User, password: string) => void
  notify: NotificationAction
  params: {
    tab: string
  }
}

interface State {
  loading: RemoteDataState
}

@ErrorHandling
export class AdminInfluxDBPage extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      loading: RemoteDataState.NotStarted,
    }
  }
  public async componentDidMount() {
    const {source, loadUsers, loadRoles, loadPermissions} = this.props

    this.setState({loading: RemoteDataState.Loading})

    if (source.authentication === SourceAuthenticationMethod.LDAP) {
      return this.setState({loading: RemoteDataState.Done})
    }

    try {
      await loadUsers(source.links.users)
      await loadPermissions(source.links.permissions)
      this.setState({loading: RemoteDataState.Done})
    } catch (error) {
      console.error(error)
      this.setState({loading: RemoteDataState.Error})
    }

    if (source.links.roles) {
      try {
        await loadRoles(source.links.roles)
      } catch (error) {
        this.setState({loading: RemoteDataState.Error})
        console.error('could not load roles: ', error)
      }
    }
  }

  public render() {
    return (
      <div className="page">
        <PageHeader titleText="InfluxDB Admin" sourceIndicator={true} />
        <FancyScrollbar className="page-contents">{this.admin}</FancyScrollbar>
      </div>
    )
  }

  private get admin(): JSX.Element {
    const {source, params} = this.props
    const {loading} = this.state
    if (loading === RemoteDataState.Loading) {
      return <div className="page-spinner" />
    }

    return (
      <div className="container-fluid">
        <SubSections
          parentUrl="admin-influxdb"
          sourceID={source.id}
          activeSection={params.tab}
          sections={this.adminSubSections}
        />
      </div>
    )
  }

  private handleClickCreate = type => () => {
    if (type === 'users') {
      this.props.addUser()
    } else if (type === 'roles') {
      this.props.addRole()
    }
  }

  private handleEditUser = (user, updates) => {
    this.props.editUser(user, updates)
  }

  private handleEditRole = (role, updates) => {
    this.props.editRole(role, updates)
  }

  private handleSaveUser = async user => {
    const {notify} = this.props
    if (!isValidUser(user)) {
      notify(notifyDBUserNamePasswordInvalid())
      return
    }
    if (user.isNew) {
      this.props.createUser(this.props.source.links.users, user)
    } else {
      // TODO update user
    }
  }

  private handleSaveRole = async role => {
    const {notify} = this.props
    if (!isValidRole(role)) {
      notify(notifyRoleNameInvalid())
      return
    }
    if (role.isNew) {
      this.props.createRole(this.props.source.links.roles, role)
    } else {
      // TODO update role
    }
  }

  private handleCancelEditUser = user => {
    this.props.removeUser(user)
  }

  private handleCancelEditRole = role => {
    this.props.removeRole(role)
  }

  private handleDeleteRole = role => {
    this.props.deleteRole(role)
  }

  private handleDeleteUser = user => {
    this.props.deleteUser(user)
  }

  private handleUpdateRoleUsers = (role, users) => {
    this.props.updateRoleUsers(role, users)
  }

  private handleUpdateRolePermissions = (role, permissions) => {
    this.props.updateRolePermissions(role, permissions)
  }

  private handleUpdateUserPermissions = (user, permissions) => {
    this.props.updateUserPermissions(user, permissions)
  }

  private handleUpdateUserRoles = (user, roles) => {
    this.props.updateUserRoles(user, roles)
  }

  private handleUpdateUserPassword = (user, password) => {
    this.props.updateUserPassword(user, password)
  }

  private get allowed(): InfluxDBPermissions[] {
    const {permissions} = this.props
    const globalPermissions = permissions.find(p => p.scope === 'all')
    return globalPermissions ? globalPermissions.allowed : []
  }

  private get hasRoles(): boolean {
    return !!this.props.source.links.roles
  }

  private get isLDAP(): boolean {
    const {source} = this.props
    return source.authentication === SourceAuthenticationMethod.LDAP
  }

  private get adminSubSections() {
    const {users, roles, source, filterUsers, filterRoles} = this.props
    return [
      {
        url: 'databases',
        name: 'Databases',
        enabled: true,
        component: <DatabaseManagerPage source={source} />,
      },
      {
        url: 'users',
        name: 'Users',
        enabled: !this.isLDAP,
        component: (
          <UsersTable
            users={users}
            allRoles={roles}
            hasRoles={this.hasRoles}
            permissions={this.allowed}
            isEditing={users.some(u => u.isEditing)}
            onSave={this.handleSaveUser}
            onCancel={this.handleCancelEditUser}
            onClickCreate={this.handleClickCreate}
            onEdit={this.handleEditUser}
            onDelete={this.handleDeleteUser}
            onFilter={filterUsers}
            onUpdatePermissions={this.handleUpdateUserPermissions}
            onUpdateRoles={this.handleUpdateUserRoles}
            onUpdatePassword={this.handleUpdateUserPassword}
          />
        ),
      },
      {
        url: 'roles',
        name: 'Roles',
        enabled: this.hasRoles && !this.isLDAP,
        component: (
          <RolesTable
            roles={roles}
            allUsers={users}
            permissions={this.allowed}
            isEditing={roles.some(r => r.isEditing)}
            onClickCreate={this.handleClickCreate}
            onEdit={this.handleEditRole}
            onSave={this.handleSaveRole}
            onCancel={this.handleCancelEditRole}
            onDelete={this.handleDeleteRole}
            onFilter={filterRoles}
            onUpdateRoleUsers={this.handleUpdateRoleUsers}
            onUpdateRolePermissions={this.handleUpdateRolePermissions}
          />
        ),
      },
      {
        url: 'queries',
        name: 'Queries',
        enabled: true,
        component: <QueriesPage source={source} />,
      },
    ]
  }
}

const mapStateToProps = ({adminInfluxDB: {users, roles, permissions}}) => ({
  users,
  roles,
  permissions,
})

const mapDispatchToProps = dispatch => ({
  loadUsers: bindActionCreators(loadUsersAsync, dispatch),
  loadRoles: bindActionCreators(loadRolesAsync, dispatch),
  loadPermissions: bindActionCreators(loadPermissionsAsync, dispatch),
  addUser: bindActionCreators(addUser, dispatch),
  addRole: bindActionCreators(addRole, dispatch),
  removeUser: bindActionCreators(deleteUser, dispatch),
  removeRole: bindActionCreators(deleteRole, dispatch),
  editUser: bindActionCreators(editUser, dispatch),
  editRole: bindActionCreators(editRole, dispatch),
  createUser: bindActionCreators(createUserAsync, dispatch),
  createRole: bindActionCreators(createRoleAsync, dispatch),
  deleteUser: bindActionCreators(deleteUserAsync, dispatch),
  deleteRole: bindActionCreators(deleteRoleAsync, dispatch),
  filterUsers: bindActionCreators(filterUsersAction, dispatch),
  filterRoles: bindActionCreators(filterRolesAction, dispatch),
  updateRoleUsers: bindActionCreators(updateRoleUsersAsync, dispatch),
  updateRolePermissions: bindActionCreators(
    updateRolePermissionsAsync,
    dispatch
  ),
  updateUserPermissions: bindActionCreators(
    updateUserPermissionsAsync,
    dispatch
  ),
  updateUserRoles: bindActionCreators(updateUserRolesAsync, dispatch),
  updateUserPassword: bindActionCreators(updateUserPasswordAsync, dispatch),
  notify: bindActionCreators(notifyAction, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(AdminInfluxDBPage)
