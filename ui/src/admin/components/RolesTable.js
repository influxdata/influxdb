import React, {PropTypes} from 'react'
import Dropdown from 'src/shared/components/MultiSelectDropdown'
import _ from 'lodash'

// TODO: replace with actual GLOBAL permissions endpoint when we get those from enterprise team
const PERMISSIONS = [
  "NoPermissions",
  "ViewAdmin",
  "ViewChronograf",
  "CreateDatabase",
  "CreateUserAndRole",
  "AddRemoveNode",
  "DropDatabase",
  "DropData",
  "ReadData",
  "WriteData",
  "Rebalance",
  "ManageShard",
  "ManageContinuousQuery",
  "ManageQuery",
  "ManageSubscription",
  "Monitor",
  "CopyShard",
  "KapacitorAPI",
  "KapacitorConfigAPI"
]

const RolesTable = ({roles}) => {
  return (
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
              roles.length ? roles.map((role) => (
                <tr key={role.name}>
                  <td>
                    {role.name}
                  </td>
                  <td>
                    {console.log(role.permissions[0].allowed)}
                    <Dropdown selectedItems={_.get(role, ['permissions', '0', 'allowed'], [])} items={PERMISSIONS} onApply={() => {}}/>
                  </td>
                  <td>
                    {role.users && role.users.map((u) => u.name).join(', ')}
                  </td>
                  <td className="text-right">
                    <button className="btn btn-xs btn-danger admin-table--delete">Delete</button>
                  </td>
                </tr>
              )) : (() => (
                <tr className="table-empty-state">
                  <th colSpan="5">
                    <p>You don&#39;t have any Roles,<br/>why not create one?</p>
                  </th>
                </tr>
              ))()
            }
          </tbody>
        </table>
      </div>
    </div>
  )
}

const {
  arrayOf,
  shape,
  string,
} = PropTypes

RolesTable.propTypes = {
  roles: arrayOf(shape({
    name: string.isRequired,
    users: arrayOf(shape({
      name: string,
    })),
    permissions: arrayOf(shape({
      name: string,
      scope: string.isRequired,
    })),
  })),
}

export default RolesTable
