import React, {PropTypes} from 'react'
import UserRow from 'src/admin/components/UserRow'
import EmptyRow from 'src/admin/components/EmptyRow'

const UsersTable = ({users, onDelete}) => (
  <div className="panel panel-info">
    <div className="panel-body">
      <table className="table v-center admin-table">
        <thead>
          <tr>
            <th>User</th>
            <th>Roles</th>
            <th>Permissions</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {
            users.length ?
              users.map((user) =>
                <UserRow key={user.name} user={user} onDelete={onDelete} />
              ) : <EmptyRow tableName={'Users'} />
          }
        </tbody>
      </table>
    </div>
  </div>
)

const {
  arrayOf,
  func,
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
  onDelete: func.isRequired,
}

export default UsersTable
