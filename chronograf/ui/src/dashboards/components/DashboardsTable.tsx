import React, {PureComponent, MouseEvent} from 'react'
import {Link, withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

import ConfirmButton from 'src/shared/components/ConfirmButton'

import {Dashboard} from 'src/types/v2'

interface Props {
  dashboards: Dashboard[]
  onDeleteDashboard: (dashboard: Dashboard) => () => void
  onCreateDashboard: () => void
  onCloneDashboard: (
    dashboard: Dashboard
  ) => (event: MouseEvent<HTMLButtonElement>) => void
  onExportDashboard: (dashboard: Dashboard) => () => void
}

class DashboardsTable extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {
      dashboards,
      onCloneDashboard,
      onDeleteDashboard,
      onExportDashboard,
    } = this.props

    if (!dashboards.length) {
      return this.emptyStateDashboard
    }

    return (
      <table className="table v-center admin-table table-highlight">
        <thead>
          <tr>
            <th>Name</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {_.sortBy(dashboards, d => d.name.toLowerCase()).map(dashboard => (
            <tr key={dashboard.id}>
              <td>
                <Link to={`/dashboards/${dashboard.id}?${this.sourceParam}`}>
                  {dashboard.name}
                </Link>
              </td>
              <td className="text-right">
                <button
                  className="btn btn-xs btn-default table--show-on-row-hover"
                  onClick={onExportDashboard(dashboard)}
                >
                  <span className="icon export" />Export
                </button>
                <button
                  className="btn btn-xs btn-default table--show-on-row-hover"
                  onClick={onCloneDashboard(dashboard)}
                >
                  <span className="icon duplicate" />
                  Clone
                </button>
                <ConfirmButton
                  confirmAction={onDeleteDashboard(dashboard)}
                  size="btn-xs"
                  type="btn-danger"
                  text="Delete"
                  customClass="table--show-on-row-hover"
                />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    )
  }

  private get sourceParam(): string {
    const {query} = this.props.location

    if (!query.sourceID) {
      return ''
    }

    return `sourceID=${query.sourceID}`
  }

  private get emptyStateDashboard(): JSX.Element {
    const {onCreateDashboard} = this.props
    return (
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
    )
  }
}

export default withRouter(DashboardsTable)
