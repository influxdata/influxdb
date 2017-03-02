import React, {PropTypes} from 'react'

const UsersTable = ({users}) => (
  <div className="panel panel-minimal">
    <div className="panel-heading u-flex u-ai-center u-jc-space-between">
      <h2 className="panel-title">Database Users</h2>
    </div>
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
            users.map((user) => {
              return (
                <tr key={user.name}>
                  <td>{user.name}</td>
                  <td>{user.roles/* .map((r) => r.name).join(', ') */}</td>
                  <td>{user.permissions.map((p) => p.scope).join(', ')}</td>
                </tr>
              );
            })
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
  })),
}

export default UsersTable
