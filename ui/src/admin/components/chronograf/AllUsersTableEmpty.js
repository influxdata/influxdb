import React from 'react'

import UsersTableHeader from 'src/admin/components/chronograf/UsersTableHeader'

import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'

const AllUsersTableEmpty = () => {
  const {
    colRole,
    colSuperAdmin,
    colProvider,
    colScheme,
    colActions,
  } = USERS_TABLE

  return (
    <div className="panel panel-default">
      <UsersTableHeader />
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
          <tbody />
        </table>
      </div>
    </div>
  )
}

export default AllUsersTableEmpty
