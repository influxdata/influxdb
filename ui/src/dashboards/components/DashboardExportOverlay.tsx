import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Components
import ExportOverlay from 'src/shared/components/ExportOverlay'

// Actions
import {convertToTemplate as convertToTemplateAction} from 'src/dashboards/actions/thunks'
import {clearExportTemplate as clearExportTemplateAction} from 'src/templates/actions/thunks'

// Types
import {AppState} from 'src/types'

import {
  dashboardCopySuccess,
  dashboardCopyFailed,
} from 'src/shared/copy/notifications'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps &
  RouteComponentProps<{orgID: string; dashboardID: string}>

class DashboardExportOverlay extends PureComponent<Props> {
  public componentDidMount() {
    const {
      match: {
        params: {dashboardID},
      },
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

const mstp = (state: AppState) => ({
  dashboardTemplate: state.resources.templates.exportTemplate.item,
  status: state.resources.templates.exportTemplate.status,
})

const mdtp = {
  convertToTemplate: convertToTemplateAction,
  clearExportTemplate: clearExportTemplateAction,
}

const connector = connect(mstp, mdtp)

export default connector(withRouter(DashboardExportOverlay))
