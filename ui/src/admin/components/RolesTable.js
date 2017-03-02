import React, {PropTypes} from 'react'

const RolesTable = ({roles}) => (
  <div className="panel panel-minimal">
    <div className="panel-body">
      <table className="table v-center">
        <thead>
          <tr>
            <th>Name</th>
            <th>Permissions</th>
            <th>Users</th>
          </tr>
        </thead>
        <tbody>
          {
            roles.length ? roles.map((role) => (
              <tr key={role.name}>
                <td>{role.name}</td>
                <td>{role.permissions && role.permissions.map((p) => p.scope).join(', ')}</td>
                <td>{role.users && role.users.map((u) => u.name).join(', ')}</td>
              </tr>
            )) : (() => (
              <tr className="table-empty-state">
                <th colSpan="5">
                  <p>You don&#39;t have any Roles,<br/>why not create one?</p>
                </th>
              </tr>
            ))()
          }
        </tbody>
      </table>
    </div>
  </div>
)

const {
  arrayOf,
  shape,
  string,
} = PropTypes

RolesTable.propTypes = {
  roles: arrayOf(shape({
    name: string.isRequired,
    users: arrayOf(shape({
      name: string,
    })),
    permissions: arrayOf(shape({
      name: string,
      scope: string.isRequired,
    })),
  })),
}

export default RolesTable
