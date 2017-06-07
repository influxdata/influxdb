import React, {PropTypes} from 'react'
import RoleRow from 'src/admin/components/RoleRow'
import EmptyRow from 'src/admin/components/EmptyRow'
import FilterBar from 'src/admin/components/FilterBar'

const RolesTable = ({
  roles,
  allUsers,
  permissions,
  isEditing,
  onClickCreate,
  onEdit,
  onSave,
  onCancel,
  onDelete,
  onFilter,
  onUpdateRoleUsers,
  onUpdateRolePermissions,
}) =>
  <div className="panel panel-default">
    <FilterBar
      type="roles"
      onFilter={onFilter}
      isEditing={isEditing}
      onClickCreate={onClickCreate}
    />
    <div className="panel-body">
      <table className="table v-center admin-table table-highlight">
        <thead>
          <tr>
            <th>Name</th>
            <th className="admin-table--left-offset">Permissions</th>
            <th className="admin-table--left-offset">Users</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {roles.length
            ? roles
                .filter(r => !r.hidden)
                .map(role =>
                  <RoleRow
                    key={role.links.self}
                    allUsers={allUsers}
                    allPermissions={permissions}
                    role={role}
                    onEdit={onEdit}
                    onSave={onSave}
                    onCancel={onCancel}
                    onDelete={onDelete}
                    onUpdateRoleUsers={onUpdateRoleUsers}
                    onUpdateRolePermissions={onUpdateRolePermissions}
                    isEditing={role.isEditing}
                    isNew={role.isNew}
                  />
                )
            : <EmptyRow tableName={'Roles'} />}
        </tbody>
      </table>
    </div>
  </div>

const {arrayOf, bool, func, shape, string} = PropTypes

RolesTable.propTypes = {
  roles: arrayOf(
    shape({
      name: string.isRequired,
      permissions: arrayOf(
        shape({
          name: string,
          scope: string.isRequired,
        })
      ),
      users: arrayOf(
        shape({
          name: string,
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
  allUsers: arrayOf(shape()),
  permissions: arrayOf(string),
  onUpdateRoleUsers: func.isRequired,
  onUpdateRolePermissions: func.isRequired,
}

export default RolesTable
