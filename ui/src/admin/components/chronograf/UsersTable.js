import React, {Component, PropTypes} from 'react'

import Authorized, {SUPERADMIN_ROLE} from 'src/auth/Authorized'

import UsersTableHeader from 'src/admin/components/chronograf/UsersTableHeader'
import OrgTableRow from 'src/admin/components/chronograf/OrgTableRow'
import NewUserTableRow from 'src/admin/components/chronograf/NewUserTableRow'

import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'

class UsersTable extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isCreatingUser: false,
    }
  }

  handleChangeUserRole = (user, currentRole) => newRole => {
    this.props.onUpdateUserRole(user, currentRole, newRole)
  }

  handleChangeSuperAdmin = (user, currentStatus) => newStatus => {
    this.props.onUpdateUserSuperAdmin(user, currentStatus, newStatus)
  }

  handleClickCreateUser = () => {
    this.setState({isCreatingUser: true})
  }

  handleBlurCreateUserRow = () => {
    this.setState({isCreatingUser: false})
  }

  render() {
    const {organization, users, onCreateUser} = this.props

    const {isCreatingUser} = this.state
    const {
      colRole,
      colSuperAdmin,
      colProvider,
      colScheme,
      colActions,
    } = USERS_TABLE

    return (
      <div className="panel panel-minimal">
        <UsersTableHeader
          numUsers={users.length}
          onClickCreateUser={this.handleClickCreateUser}
        />
        <div className="panel-body">
          <table className="table table-highlight v-center chronograf-admin-table">
            <thead>
              <tr>
                <th>Username</th>
                <th style={{width: colRole}} className="align-with-col-text">
                  Role
                </th>
                <Authorized requiredRole={SUPERADMIN_ROLE}>
                  <th style={{width: colSuperAdmin}} className="text-center">
                    SuperAdmin
                  </th>
                </Authorized>
                <th style={{width: colProvider}}>Provider</th>
                <th style={{width: colScheme}}>Scheme</th>
                <th className="text-right" style={{width: colActions}} />
              </tr>
            </thead>
            <tbody>
              {isCreatingUser
                ? <NewUserTableRow
                    organization={organization}
                    onBlur={this.handleBlurCreateUserRow}
                    onCreateUser={onCreateUser}
                  />
                : null}
              {users.length || !isCreatingUser
                ? users.map((user, i) =>
                    <OrgTableRow
                      user={user}
                      key={i}
                      organization={organization}
                      onChangeUserRole={this.handleChangeUserRole}
                      onChangeSuperAdmin={this.handleChangeSuperAdmin}
                    />
                  )
                : <tr className="table-empty-state">
                    <Authorized
                      requiredRole={SUPERADMIN_ROLE}
                      replaceWithIfNotAuthorized={
                        <th colSpan="5">
                          <p>No Users to display</p>
                        </th>
                      }
                    >
                      <th colSpan="6">
                        <p>No Users to display</p>
                      </th>
                    </Authorized>
                  </tr>}
            </tbody>
          </table>
        </div>
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

UsersTable.propTypes = {
  users: arrayOf(shape()),
  organization: shape({
    name: string.isRequired,
    id: string.isRequired,
  }),
  onCreateUser: func.isRequired,
  onUpdateUserRole: func.isRequired,
  onUpdateUserSuperAdmin: func.isRequired,
}
export default UsersTable
