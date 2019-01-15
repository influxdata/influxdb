// Libraries
import React, {Component} from 'react'
import _ from 'lodash'

// Components
import Table from 'src/dashboards/components/dashboard_index/Table'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Dashboard, Organization} from 'src/types/v2'
import {Notification} from 'src/types/notifications'

interface Props {
  dashboards: Dashboard[]
  orgs: Organization[]
  defaultDashboardLink: string
  onSetDefaultDashboard: (dashboardLink: string) => void
  onCreateDashboard: () => void
  onCloneDashboard: (dashboard: Dashboard) => void
  onExportDashboard: (dashboard: Dashboard) => void
  onDeleteDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
  onEditLabels: (dashboard: Dashboard) => void
  notify: (message: Notification) => void
  searchTerm: string
}

@ErrorHandling
export default class DashboardsIndexContents extends Component<Props> {
  public render() {
    const {
      onDeleteDashboard,
      onCloneDashboard,
      onExportDashboard,
      onCreateDashboard,
      defaultDashboardLink,
      onSetDefaultDashboard,
      onUpdateDashboard,
      onEditLabels,
      searchTerm,
      orgs,
    } = this.props

    return (
      <div className="col-md-12">
        <Table
          searchTerm={searchTerm}
          dashboards={this.filteredDashboards}
          onDeleteDashboard={onDeleteDashboard}
          onCreateDashboard={onCreateDashboard}
          onCloneDashboard={onCloneDashboard}
          onExportDashboard={onExportDashboard}
          defaultDashboardLink={defaultDashboardLink}
          onSetDefaultDashboard={onSetDefaultDashboard}
          onUpdateDashboard={onUpdateDashboard}
          onEditLabels={onEditLabels}
          orgs={orgs}
        />
      </div>
    )
  }

  private get filteredDashboards(): Dashboard[] {
    const {dashboards, searchTerm} = this.props

    const matchingDashboards = dashboards.filter(d =>
      d.name.toLowerCase().includes(searchTerm.toLowerCase())
    )

    return _.sortBy(matchingDashboards, d => d.name.toLowerCase())
  }
}
