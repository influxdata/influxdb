import React, {PropTypes} from 'react'
import UserRow from 'src/admin/components/UserRow'
import EmptyRow from 'src/admin/components/EmptyRow'

const UsersTable = ({users}) => (
  <div className="panel panel-info">
    <div className="panel-heading u-flex u-ai-center u-jc-space-between">
      <div className="users__search-widget input-group admin__search-widget">
        <input
          type="text"
          className="form-control"
          placeholder="Filter Role..."
        />
        <div className="input-group-addon">
          <span className="icon search" aria-hidden="true"></span>
        </div>
      </div>
      <a href="#" className="btn btn-primary">Create User</a>
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
