import React, {Component, PropTypes} from 'react'

import uuid from 'node-uuid'

import AllUsersTableHeader from 'src/admin/components/chronograf/AllUsersTableHeader'
import AllUsersTableRowNew from 'src/admin/components/chronograf/AllUsersTableRowNew'
import AllUsersTableRow from 'src/admin/components/chronograf/AllUsersTableRow'

import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'

class AllUsersTable extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isCreatingUser: false,
    }
  }

  handleChangeUserRole = (user, currentRole) => newRole => {
    this.props.onUpdateUserRole(user, currentRole, newRole)
  }

  handleChangeSuperAdmin = user => newStatus => {
    this.props.onUpdateUserSuperAdmin(user, newStatus)
  }

  handleDeleteUser = user => {
    this.props.onDeleteUser(user)
  }

  handleClickCreateUser = () => {
    this.setState({isCreatingUser: true})
  }

  handleBlurCreateUserRow = () => {
    this.setState({isCreatingUser: false})
  }

  render() {
    const {organization, users, onCreateUser, meID, notify} = this.props

    const {isCreatingUser} = this.state
    const {
      colRole,
      colSuperAdmin,
      colProvider,
      colScheme,
      colActions,
    } = USERS_TABLE

    return (
      <div className="panel panel-default">
        <AllUsersTableHeader
          numUsers={users.length}
          onClickCreateUser={this.handleClickCreateUser}
          isCreatingUser={isCreatingUser}
          organization={organization}
        />
        <div className="panel-body">
          <table className="table table-highlight v-center chronograf-admin-table">
            <thead>
              <tr>
                <th>Username</th>
                <th style={{width: colRole}} className="align-with-col-text">
                  Role
                </th>
                <th style={{width: colSuperAdmin}} className="text-center">
                  SuperAdmin
                </th>
                <th style={{width: colProvider}}>Provider</th>
                <th style={{width: colScheme}}>Scheme</th>
                <th className="text-right" style={{width: colActions}} />
              </tr>
            </thead>
            <tbody>
              {isCreatingUser
                ? <AllUsersTableRowNew
                    organization={organization}
                    onBlur={this.handleBlurCreateUserRow}
                    onCreateUser={onCreateUser}
                    notify={notify}
                  />
                : null}
              {users.length || !isCreatingUser
                ? users.map(user =>
                    <AllUsersTableRow
                      user={user}
                      key={uuid.v4()}
                      organization={organization}
                      onChangeUserRole={this.handleChangeUserRole}
                      onChangeSuperAdmin={this.handleChangeSuperAdmin}
                      onDelete={this.handleDeleteUser}
                      meID={meID}
                    />
                  )
                : <tr className="table-empty-state">
                    <th colSpan="6">
                      <p>No Users to display</p>
                    </th>
                  </tr>}
            </tbody>
          </table>
        </div>
      </div>
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

AllUsersTable.propTypes = {
  users: arrayOf(
    shape({
      id: string,
      links: shape({
        self: string.isRequired,
      }),
      name: string.isRequired,
      provider: string.isRequired,
      roles: arrayOf(
        shape({
          name: string.isRequired,
          organization: string.isRequired,
        })
      ),
      scheme: string.isRequired,
      superAdmin: bool,
    })
  ).isRequired,
  organization: shape({
    name: string.isRequired,
    id: string.isRequired,
  }),
  onCreateUser: func.isRequired,
  onUpdateUserRole: func.isRequired,
  onUpdateUserSuperAdmin: func.isRequired,
  onDeleteUser: func.isRequired,
  meID: string.isRequired,
  notify: func.isRequired,
}

export default AllUsersTable
