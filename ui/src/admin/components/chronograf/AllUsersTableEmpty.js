import React from 'react'

import UsersTableHeader from 'src/admin/components/chronograf/UsersTableHeader'

import {USERS_TABLE} from 'src/admin/constants/chronografTableSizing'
const {
  colRole,
  colOrganizations,
  colSuperAdmin,
  colProvider,
  colScheme,
  colActions,
} = USERS_TABLE

const AllUsersTableEmpty = () =>
  <div className="panel panel-default">
    <UsersTableHeader />
    <div className="panel-body">
      <table className="table table-highlight v-center chronograf-admin-table">
        <thead>
          <tr>
            <th>Username</th>
            <th
              style={{width: colOrganizations}}
              className="align-with-col-text"
            >
              Organizations
            </th>
            <th style={{width: colProvider}}>Provider</th>
            <th style={{width: colScheme}}>Scheme</th>
            <th style={{width: colSuperAdmin}} className="text-center">
              SuperAdmin
            </th>
            <th className="align-with-col-text" style={{width: colRole}} />
            <th className="text-right" style={{width: colActions}} />
          </tr>
        </thead>
        <tbody />
      </table>
    </div>
  </div>

export default AllUsersTableEmpty
