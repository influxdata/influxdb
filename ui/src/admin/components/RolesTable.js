import React, {PropTypes} from 'react'
import RoleRow from 'src/admin/components/RoleRow'
import EmptyRow from 'src/admin/components/EmptyRow'

const RolesTable = ({roles, onDelete}) => (
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
      <a href="#" className="btn btn-primary">Create Role</a>
    </div>
    <div className="panel-body">
      <table className="table v-center admin-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Permissions</th>
            <th>Users</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {
            roles.length ?
              roles.map((role) =>
                <RoleRow key={role.name} role={role} onDelete={onDelete} />
              ) : <EmptyRow tableName={'Roles'} />
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

RolesTable.propTypes = {
  roles: arrayOf(shape({
    name: string.isRequired,
    permissions: arrayOf(shape({
      name: string,
      scope: string.isRequired,
    })),
    users: arrayOf(shape({
      name: string,
    })),
  })),
  onDelete: func.isRequired,
}

export default RolesTable
