import React, {PropTypes} from 'react'
import RoleRow from 'src/admin/components/RoleRow'
import EmptyRow from 'src/admin/components/EmptyRow'

const RolesTable = ({roles}) => (
  <div className="panel panel-minimal">
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
            roles.length ?
              roles.map((role) =>
                <RoleRow key={role.name} role={role} />
              ) : <EmptyRow tableName={'Roles'} />
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
}

export default RolesTable
