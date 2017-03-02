import React, {PropTypes} from 'react'

const RolesTable = ({roles}) => (
  <div className="panel panel-minimal">
    <div className="panel-heading u-flex u-ai-center u-jc-space-between">
      <h2 className="panel-title">Database Roles</h2>
    </div>
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
            roles.map((role) => {
              return (
                <tr key={role.name}>
                  <td>{role.name}</td>
                  <td>{role.permissions.map((p) => p.name).join(', ')}</td>
                  <td>{role.users.map((u) => u.name).join(', ')}</td>
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
} = PropTypes

RolesTable.propTypes = {
  users: arrayOf(shape()),
  roles: arrayOf(shape()),
}

export default RolesTable
