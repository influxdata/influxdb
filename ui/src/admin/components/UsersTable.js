import React, {PropTypes} from 'react'

const UsersTable = ({users}) => (
  <div className="panel panel-minimal">
    <div className="panel-body">
      <table className="table v-center">
        <thead>
          <tr>
            <th>User</th>
            <th>Roles</th>
            <th>Permissions</th>
          </tr>
        </thead>
        <tbody>
          {
            users.length ? users.map((user) => (
              <tr key={user.name}>
                <td>{user.name}</td>
                <td>{user.roles.map((r) => r.name).join(', ')}</td>
                <td>{user.permissions.map((p) => p.scope).join(', ')}</td>
              </tr>
            )) : (() => (
              <tr className="table-empty-state">
                <th colSpan="5">
                  <p>You don&#39;t have any Users,<br/>why not create one?</p>
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

UsersTable.propTypes = {
  users: arrayOf(shape({
    name: string.isRequired,
    roles: arrayOf(shape({
      name: string,
    })),
    permissions: arrayOf(shape({
      name: string,
      scope: string.isRequired,
    })),
  })),
}

export default UsersTable
