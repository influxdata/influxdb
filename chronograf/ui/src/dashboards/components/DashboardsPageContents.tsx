import React, {Component, MouseEvent} from 'react'

import DashboardsTable from 'src/dashboards/components/DashboardsTable'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {Dashboard} from 'src/types/v2'
import {Notification} from 'src/types/notifications'

interface Props {
  dashboards: Dashboard[]
  defaultDashboardLink: string
  onSetDefaultDashboard: (dashboardLink: string) => void
  onDeleteDashboard: (dashboard: Dashboard) => () => void
  onCreateDashboard: () => void
  onCloneDashboard: (
    dashboard: Dashboard
  ) => (event: MouseEvent<HTMLButtonElement>) => void
  onExportDashboard: (dashboard: Dashboard) => () => void
  notify: (message: Notification) => void
  searchTerm: string
}

@ErrorHandling
class DashboardsPageContents extends Component<Props> {
  public render() {
    const {
      onDeleteDashboard,
      onCloneDashboard,
      onExportDashboard,
      onCreateDashboard,
      defaultDashboardLink,
      onSetDefaultDashboard,
    } = this.props

    return (
      <div className="col-md-12">
        <div className="panel">
          <div className="panel-body">
            <DashboardsTable
              dashboards={this.filteredDashboards}
              onDeleteDashboard={onDeleteDashboard}
              onCreateDashboard={onCreateDashboard}
              onCloneDashboard={onCloneDashboard}
              onExportDashboard={onExportDashboard}
              defaultDashboardLink={defaultDashboardLink}
              onSetDefaultDashboard={onSetDefaultDashboard}
            />
          </div>
        </div>
      </div>
    )
  }

  private get filteredDashboards(): Dashboard[] {
    const {dashboards, searchTerm} = this.props

    return dashboards.filter(d =>
      d.name.toLowerCase().includes(searchTerm.toLowerCase())
    )
  }
}

export default DashboardsPageContents
