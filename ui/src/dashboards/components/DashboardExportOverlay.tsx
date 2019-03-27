import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import ExportOverlay from 'src/shared/components/ExportOverlay'

// Actions
import {convertToTemplate as convertToTemplateAction} from 'src/dashboards/actions/index'
import {clearExportTemplate as clearExportTemplateAction} from 'src/templates/actions'

// Types
import {DocumentCreate} from '@influxdata/influx'
import {AppState} from 'src/types'
import {RemoteDataState} from 'src/types'

interface OwnProps {
  params: {dashboardID: string; orgID: string}
}

interface DispatchProps {
  convertToTemplate: typeof convertToTemplateAction
  clearExportTemplate: typeof clearExportTemplateAction
}

interface StateProps {
  dashboardTemplate: DocumentCreate
  status: RemoteDataState
  orgID: string
}

type Props = OwnProps & StateProps & DispatchProps & WithRouterProps

class DashboardExportOverlay extends PureComponent<Props> {
  public async componentDidMount() {
    const {
      params: {dashboardID},
      convertToTemplate,
    } = this.props

    convertToTemplate(dashboardID)
  }

  public render() {
    const {status, dashboardTemplate} = this.props

    return (
      <ExportOverlay
        resourceName="Dashboard"
        resource={dashboardTemplate}
        onDismissOverlay={this.onDismiss}
        orgID={this.orgID}
        status={status}
      />
    )
  }

  private get orgID() {
    const orgFromExistingResource = this.props.orgID
    const orgInRoutes = this.props.params.orgID
    return orgFromExistingResource || orgInRoutes
  }

  private onDismiss = () => {
    const {router, clearExportTemplate} = this.props

    router.goBack()
    clearExportTemplate()
  }
}

const mstp = (state: AppState): StateProps => ({
  dashboardTemplate: state.templates.exportTemplate.item,
  status: state.templates.exportTemplate.status,
  orgID: state.templates.exportTemplate.orgID,
})

const mdtp: DispatchProps = {
  convertToTemplate: convertToTemplateAction,
  clearExportTemplate: clearExportTemplateAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<Props>(DashboardExportOverlay))
