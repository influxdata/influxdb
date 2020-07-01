import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Components
import ExportOverlay from 'src/shared/components/ExportOverlay'

// Actions
import {convertToTemplate as convertToTemplateAction} from 'src/dashboards/actions/thunks'
import {clearExportTemplate as clearExportTemplateAction} from 'src/templates/actions/thunks'

// Types
import {DocumentCreate} from '@influxdata/influx'
import {AppState} from 'src/types'
import {RemoteDataState} from 'src/types'

import {
  dashboardCopySuccess,
  dashboardCopyFailed,
} from 'src/shared/copy/notifications'

interface OwnProps {
  match: {dashboardID: string}
}

interface DispatchProps {
  convertToTemplate: typeof convertToTemplateAction
  clearExportTemplate: typeof clearExportTemplateAction
}

interface StateProps {
  dashboardTemplate: DocumentCreate
  status: RemoteDataState
}

type Props = OwnProps &
  StateProps &
  DispatchProps &
  RouteComponentProps<{orgID: string}>

class DashboardExportOverlay extends PureComponent<Props> {
  public componentDidMount() {
    const {
      match: {dashboardID},
      convertToTemplate,
    } = this.props

    convertToTemplate(dashboardID)
  }

  public render() {
    const {status, dashboardTemplate} = this.props

    const notes = (_text, success) => {
      if (success) {
        return dashboardCopySuccess()
      }

      return dashboardCopyFailed()
    }

    return (
      <ExportOverlay
        resourceName="Dashboard"
        resource={dashboardTemplate}
        onDismissOverlay={this.onDismiss}
        onCopyText={notes}
        status={status}
      />
    )
  }

  private onDismiss = () => {
    const {history, clearExportTemplate} = this.props

    history.goBack()
    clearExportTemplate()
  }
}

const mstp = (state: AppState): StateProps => ({
  dashboardTemplate: state.resources.templates.exportTemplate.item,
  status: state.resources.templates.exportTemplate.status,
})

const mdtp: DispatchProps = {
  convertToTemplate: convertToTemplateAction,
  clearExportTemplate: clearExportTemplateAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter(DashboardExportOverlay))
