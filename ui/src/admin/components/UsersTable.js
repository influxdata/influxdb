import React, {PropTypes} from 'react'
import UserRow from 'src/admin/components/UserRow'
import EmptyRow from 'src/admin/components/EmptyRow'

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
            users.length ?
              users.map((user) =>
                <UserRow key={user.name} user={user} />
              ) : <EmptyRow tableName={'Users'} />
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
