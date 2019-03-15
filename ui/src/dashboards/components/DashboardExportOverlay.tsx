import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import ExportOverlay from 'src/shared/components/ExportOverlay'

// Utils
import {dashboardToTemplate} from 'src/shared/utils/resourceToTemplate'

// APIs
import {getDashboard, getView} from 'src/dashboards/apis'

// Types
import {ITemplate} from '@influxdata/influx'

interface State {
  dashboardTemplate: ITemplate
  orgID: string
}

interface Props extends WithRouterProps {
  params: {dashboardID: string; orgID: string}
}

class DashboardExportOverlay extends PureComponent<Props, State> {
  public state: State = {dashboardTemplate: null, orgID: null}

  public async componentDidMount() {
    const {
      params: {dashboardID},
    } = this.props

    const dashboard = await getDashboard(dashboardID)
    const pendingViews = dashboard.cells.map(c => getView(dashboardID, c.id))
    const views = await Promise.all(pendingViews)
    const dashboardTemplate = dashboardToTemplate(dashboard, views)

    this.setState({dashboardTemplate, orgID: dashboard.orgID})
  }

  public render() {
    const {dashboardTemplate, orgID} = this.state
    if (!dashboardTemplate) {
      return null
    }
    return (
      <ExportOverlay
        resourceName="Dashboard"
        resource={dashboardTemplate}
        onDismissOverlay={this.onDismiss}
        orgID={orgID}
      />
    )
  }

  private onDismiss = () => {
    const {router} = this.props

    router.goBack()
  }
}

export default withRouter(DashboardExportOverlay)
