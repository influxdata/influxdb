import React, {Component, PropTypes} from 'react'

class ChronografAllUsersTable extends Component {
  constructor(props) {
    super(props)
  }

  renderOrgAndRoleCell = user => {
    const {onViewOrg, organizationName} = this.props

    if (!user.roles.length) {
      return <a href="#">None</a>
    }

    return organizationName
      ? user.roles
          .filter(role => role.organizationName === organizationName)
          .map((role, r) =>
            <span key={r} className="org-and-role">
              {role.name}
            </span>
          )
      : user.roles.map((role, r) =>
          <span key={r} className="org-and-role">
            <a href="#" onClick={onViewOrg(role.organizationName)}>
              {role.organizationName}
            </a>
            <i>({role.name}</i>)
          </span>
        )
  }

  renderTableRows = () => {
    const {users, organizationName} = this.props

    const filteredUsers = organizationName
      ? users.filter(user => {
          return user.roles.find(
            role => role.organizationName === organizationName
          )
        })
      : users

    return filteredUsers.map((user, u) =>
      <tr key={u}>
        <td>
          <div className="dark-checkbox">
            <input
              type="checkbox"
              id={`chronograf-user-${u}`}
              className="form-control-static"
              value="off"
            />
            <label htmlFor={`chronograf-user-${u}`} />
          </div>
        </td>
        <td>
          <strong>
            {user.name}
          </strong>
        </td>
        <td>
          {user.superadmin ? 'Yes' : '--'}
        </td>
        <td>
          {this.renderOrgAndRoleCell(user)}
        </td>
        <td>
          {user.provider}
        </td>
        <td className="text-right">
          {user.scheme}
        </td>
      </tr>
    )
  }

  render() {
    const {organizationName} = this.props

    return (
      <table className="table table-highlight">
        <thead>
          <tr>
            <th>
              <div className="dark-checkbox">
                <input
                  type="checkbox"
                  id="chronograf-user-all"
                  className="form-control-static"
                  value="off"
                />
                <label htmlFor="chronograf-user-all" />
              </div>
            </th>
            <th>Username</th>
            <th>SuperAdmin</th>
            <th>
              {organizationName ? 'Role' : 'Organization & Role'}
            </th>
            <th>Provider</th>
            <th className="text-right">Scheme</th>
          </tr>
        </thead>
        <tbody>
          {this.renderTableRows()}
        </tbody>
      </table>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

ChronografAllUsersTable.propTypes = {
  users: arrayOf(shape()),
  onViewOrg: func.isRequired,
  organizationName: string,
}
export default ChronografAllUsersTable
