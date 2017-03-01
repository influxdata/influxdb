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
            <th>User Name</th>
          </tr>
        </thead>
        <tbody>
          {
            users.map((u) => {
              return <div key={u.name}>{u.name}</div>;
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
