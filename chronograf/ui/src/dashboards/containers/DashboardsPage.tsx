import React, {PureComponent} from 'react'
import {withRouter, InjectedRouter} from 'react-router'
import {connect} from 'react-redux'
import download from 'src/external/download'
import _ from 'lodash'

import DashboardsContents from 'src/dashboards/components/DashboardsPageContents'
import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'
import {getDeep} from 'src/utils/wrappers'

import {createDashboard} from 'src/dashboards/apis'
import {
  getDashboardsAsync,
  deleteDashboardAsync,
  getChronografVersion,
  importDashboardAsync,
  retainRangesDashTimeV1 as retainRangesDashTimeV1Action,
} from 'src/dashboards/actions'
import {notify as notifyAction} from 'src/shared/actions/notifications'

import {
  NEW_DASHBOARD,
  DEFAULT_DASHBOARD_NAME,
  NEW_DEFAULT_DASHBOARD_CELL,
} from 'src/dashboards/constants'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  notifyDashboardExported,
  notifyDashboardExportFailed,
} from 'src/shared/copy/notifications'

import {Source, Dashboard} from 'src/types'
import {Notification} from 'src/types/notifications'
import {DashboardFile, Cell} from 'src/types/dashboards'

interface Props {
  source: Source
  router: InjectedRouter
  handleGetDashboards: () => Dashboard[]
  handleGetChronografVersion: () => string
  handleDeleteDashboard: (dashboard: Dashboard) => void
  handleImportDashboard: (dashboard: Dashboard) => void
  notify: (message: Notification) => void
  retainRangesDashTimeV1: (dashboardIDs: number[]) => void
  dashboards: Dashboard[]
}

@ErrorHandling
class DashboardsPage extends PureComponent<Props> {
  public async componentDidMount() {
    const dashboards = await this.props.handleGetDashboards()
    const dashboardIDs = dashboards.map(d => d.id)
    this.props.retainRangesDashTimeV1(dashboardIDs)
  }

  public render() {
    const {dashboards, notify} = this.props
    const dashboardLink = `/sources/${this.props.source.id}`

    return (
      <div className="page">
        <PageHeader titleText="Dashboards" sourceIndicator={true} />
        <DashboardsContents
          dashboardLink={dashboardLink}
          dashboards={dashboards}
          onDeleteDashboard={this.handleDeleteDashboard}
          onCreateDashboard={this.handleCreateDashboard}
          onCloneDashboard={this.handleCloneDashboard}
          onExportDashboard={this.handleExportDashboard}
          onImportDashboard={this.handleImportDashboard}
          notify={notify}
        />
      </div>
    )
  }

  private handleCreateDashboard = async (): Promise<void> => {
    const {
      source: {id},
      router: {push},
    } = this.props
    const {data} = await createDashboard(NEW_DASHBOARD)
    push(`/sources/${id}/dashboards/${data.id}`)
  }

  private handleCloneDashboard = (dashboard: Dashboard) => async (): Promise<
    void
  > => {
    const {
      source: {id},
      router: {push},
    } = this.props
    const {data} = await createDashboard({
      ...dashboard,
      name: `${dashboard.name} (clone)`,
    })
    push(`/sources/${id}/dashboards/${data.id}`)
  }

  private handleDeleteDashboard = (dashboard: Dashboard) => (): void => {
    this.props.handleDeleteDashboard(dashboard)
  }

  private handleExportDashboard = (dashboard: Dashboard) => async (): Promise<
    void
  > => {
    const dashboardForDownload = await this.modifyDashboardForDownload(
      dashboard
    )
    try {
      download(
        JSON.stringify(dashboardForDownload, null, '\t'),
        `${dashboard.name}.json`,
        'text/plain'
      )
      this.props.notify(notifyDashboardExported(dashboard.name))
    } catch (error) {
      this.props.notify(notifyDashboardExportFailed(dashboard.name, error))
    }
  }

  private modifyDashboardForDownload = async (
    dashboard: Dashboard
  ): Promise<DashboardFile> => {
    const version = await this.props.handleGetChronografVersion()
    return {meta: {chronografVersion: version}, dashboard}
  }

  private handleImportDashboard = async (
    dashboard: Dashboard
  ): Promise<void> => {
    const name = _.get(dashboard, 'name', DEFAULT_DASHBOARD_NAME)
    const cellsWithDefaultsApplied = getDeep<Cell[]>(
      dashboard,
      'cells',
      []
    ).map(c => ({...NEW_DEFAULT_DASHBOARD_CELL, ...c}))

    await this.props.handleImportDashboard({
      ...dashboard,
      name,
      cells: cellsWithDefaultsApplied,
    })
  }
}

const mapStateToProps = ({dashboardUI: {dashboards, dashboard}}) => ({
  dashboards,
  dashboard,
})

const mapDispatchToProps = {
  handleGetDashboards: getDashboardsAsync,
  handleDeleteDashboard: deleteDashboardAsync,
  handleGetChronografVersion: getChronografVersion,
  handleImportDashboard: importDashboardAsync,
  notify: notifyAction,
  retainRangesDashTimeV1: retainRangesDashTimeV1Action,
}

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(DashboardsPage)
)
