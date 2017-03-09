import React, {PropTypes} from 'react'

import UserRow from 'src/admin/components/UserRow'
import EmptyRow from 'src/admin/components/EmptyRow'
import FilterBar from 'src/admin/components/FilterBar'

const UsersTable = ({users, hasRoles, isEditingUsers, onClickCreate, onEdit, onSave, onCancel, onDelete, onFilter}) => (
  <div className="panel panel-info">
    <FilterBar type="users" onFilter={onFilter} isEditing={isEditingUsers} onClickCreate={onClickCreate} />
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
              users.filter(u => !u.hidden).map(user =>
                <UserRow
                  key={user.links.self}
                  user={user}
                  onEdit={onEdit}
                  onSave={onSave}
                  onCancel={onCancel}
                  onDelete={onDelete}
                  isEditing={user.isEditing}
                  isNew={user.isNew}
                />) :
              <EmptyRow tableName={'Users'} />
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
  isEditingUsers: bool,
  onClickCreate: func.isRequired,
  onEdit: func.isRequired,
  onSave: func.isRequired,
  onCancel: func.isRequired,
  addFlashMessage: func.isRequired,
  hasRoles: bool.isRequired,
  onDelete: func.isRequired,
  onFilter: func,
}

export default UsersTable
