import React, {PropTypes} from 'react'
import UserRow from 'src/admin/components/UserRow'
import EmptyRow from 'src/admin/components/EmptyRow'
import FilterBar from 'src/admin/components/FilterBar'

const UsersTable = ({users, hasRoles, onDelete, onFilter}) => (
  <div className="panel panel-info">
    <FilterBar name="Users" onFilter={onFilter} />
    <div className="panel-body">
      <table className="table v-center admin-table">
        <thead>
          <tr>
            <th>User</th>
            {hasRoles && <th>Roles</th>}
            <th>Permissions</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {
            users.length ?
              users.filter(u => !u.hidden).map((user) =>
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
  bool,
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
  hasRoles: bool.isRequired,
  onDelete: func.isRequired,
  onFilter: func,
}

export default UsersTable
