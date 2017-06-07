import React, {PropTypes} from 'react'

import UserRow from 'src/admin/components/UserRow'
import EmptyRow from 'src/admin/components/EmptyRow'
import FilterBar from 'src/admin/components/FilterBar'

const UsersTable = ({
  users,
  allRoles,
  hasRoles,
  permissions,
  isEditing,
  onClickCreate,
  onEdit,
  onSave,
  onCancel,
  onDelete,
  onFilter,
  onUpdatePermissions,
  onUpdateRoles,
  onUpdatePassword,
}) =>
  <div className="panel panel-default">
    <FilterBar
      type="users"
      onFilter={onFilter}
      isEditing={isEditing}
      onClickCreate={onClickCreate}
    />
    <div className="panel-body">
      <table className="table v-center admin-table table-highlight">
        <thead>
          <tr>
            <th>User</th>
            <th>Password</th>
            {hasRoles && <th className="admin-table--left-offset">Roles</th>}
            <th className="admin-table--left-offset">Permissions</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {users.length
            ? users
                .filter(u => !u.hidden)
                .map(user =>
                  <UserRow
                    key={user.links.self}
                    user={user}
                    onEdit={onEdit}
                    onSave={onSave}
                    onCancel={onCancel}
                    onDelete={onDelete}
                    isEditing={user.isEditing}
                    isNew={user.isNew}
                    allRoles={allRoles}
                    hasRoles={hasRoles}
                    allPermissions={permissions}
                    onUpdatePermissions={onUpdatePermissions}
                    onUpdateRoles={onUpdateRoles}
                    onUpdatePassword={onUpdatePassword}
                  />
                )
            : <EmptyRow tableName={'Users'} />}
        </tbody>
      </table>
    </div>
  </div>

const {arrayOf, bool, func, shape, string} = PropTypes

UsersTable.propTypes = {
  users: arrayOf(
    shape({
      name: string.isRequired,
      roles: arrayOf(
        shape({
          name: string,
        })
      ),
      permissions: arrayOf(
        shape({
          name: string,
          scope: string.isRequired,
        })
      ),
    })
  ),
  isEditing: bool,
  onClickCreate: func.isRequired,
  onEdit: func.isRequired,
  onSave: func.isRequired,
  onCancel: func.isRequired,
  onDelete: func.isRequired,
  onFilter: func,
  allRoles: arrayOf(shape()),
  permissions: arrayOf(string),
  hasRoles: bool.isRequired,
  onUpdatePermissions: func,
  onUpdateRoles: func,
  onUpdatePassword: func,
}

export default UsersTable
