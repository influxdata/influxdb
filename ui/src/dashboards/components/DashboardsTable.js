import React, {PropTypes} from 'react'
import {Link} from 'react-router'
import _ from 'lodash'

import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'

import DeleteConfirmTableCell from 'shared/components/DeleteConfirmTableCell'

const AuthorizedEmptyState = ({onCreateDashboard}) =>
  <div className="generic-empty-state">
    <h4 style={{marginTop: '90px'}}>
      Looks like you don’t have any dashboards
    </h4>
    <br />
    <button
      className="btn btn-sm btn-primary"
      onClick={onCreateDashboard}
      style={{marginBottom: '90px'}}
    >
      <span className="icon plus" /> Create Dashboard
    </button>
  </div>

const unauthorizedEmptyState = (
  <div className="generic-empty-state">
    <h4 style={{margin: '90px 0'}}>Looks like you don’t have any dashboards</h4>
  </div>
)

const DashboardsTable = ({
  dashboards,
  onDeleteDashboard,
  onCreateDashboard,
  dashboardLink,
}) => {
  return dashboards && dashboards.length
    ? <table className="table v-center admin-table table-highlight">
        <thead>
          <tr>
            <th>Name</th>
            <th>Template Variables</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {_.sortBy(dashboards, d => d.name.toLowerCase()).map(dashboard =>
            <tr key={dashboard.id}>
              <td>
                <Link to={`${dashboardLink}/dashboards/${dashboard.id}`}>
                  {dashboard.name}
                </Link>
              </td>
              <td>
                {dashboard.templates.length
                  ? dashboard.templates.map(tv =>
                      <code className="table--temp-var" key={tv.id}>
                        {tv.tempVar}
                      </code>
                    )
                  : <span className="empty-string">None</span>}
              </td>
              <Authorized
                requiredRole={EDITOR_ROLE}
                replaceWithIfNotAuthorized={<td />}
              >
                <DeleteConfirmTableCell
                  onDelete={onDeleteDashboard}
                  item={dashboard}
                  buttonSize="btn-xs"
                />
              </Authorized>
            </tr>
          )}
        </tbody>
      </table>
    : <Authorized
        requiredRole={EDITOR_ROLE}
        replaceWithIfNotAuthorized={unauthorizedEmptyState}
      >
        <AuthorizedEmptyState onCreateDashboard={onCreateDashboard} />
      </Authorized>
}

const {arrayOf, func, shape, string} = PropTypes

DashboardsTable.propTypes = {
  dashboards: arrayOf(shape()),
  onDeleteDashboard: func.isRequired,
  onCreateDashboard: func.isRequired,
  dashboardLink: string.isRequired,
}

AuthorizedEmptyState.propTypes = {
  onCreateDashboard: func.isRequired,
}

export default DashboardsTable
