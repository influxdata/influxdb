// Libraries
import React, {PureComponent} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {isEmpty} from 'lodash'
import {connect} from 'react-redux'

// Components
import ImportOverlay from 'src/shared/components/ImportOverlay'

// Copy
import {invalidJSON} from 'src/shared/copy/notifications'

// Actions
import {
  getDashboards,
  createDashboardFromTemplate as createDashboardFromTemplateAction,
} from 'src/dashboards/actions/thunks'
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Types
import {ComponentStatus} from '@influxdata/clockface'

// Utils
import jsonlint from 'jsonlint-mod'

interface State {
  status: ComponentStatus
}

interface DispatchProps {
  createDashboardFromTemplate: typeof createDashboardFromTemplateAction
  notify: typeof notifyAction
  populateDashboards: typeof getDashboards
}

type Props = RouteComponentProps<{orgID: string}> & DispatchProps

class DashboardImportOverlay extends PureComponent<Props> {
  public state: State = {
    status: ComponentStatus.Default,
  }

  public render() {
    return (
      <ImportOverlay
        isVisible={true}
        onDismissOverlay={this.onDismiss}
        resourceName="Dashboard"
        onSubmit={this.handleImportDashboard}
        status={this.state.status}
        updateStatus={this.updateOverlayStatus}
      />
    )
  }

  private updateOverlayStatus = (status: ComponentStatus) =>
    this.setState(() => ({status}))

  private handleImportDashboard = (uploadContent: string) => {
    const {createDashboardFromTemplate, notify, populateDashboards} = this.props

    let template
    this.updateOverlayStatus(ComponentStatus.Default)
    try {
      template = jsonlint.parse(uploadContent)
    } catch (error) {
      this.updateOverlayStatus(ComponentStatus.Error)
      notify(invalidJSON(error.message))
      return
    }

    if (isEmpty(template)) {
      this.onDismiss()
    }

    createDashboardFromTemplate(template)
    populateDashboards()
    this.onDismiss()
  }

  private onDismiss = (): void => {
    const {history} = this.props
    history.goBack()
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
  populateDashboards: getDashboards,
  createDashboardFromTemplate: createDashboardFromTemplateAction,
}

export default connect<{}, DispatchProps>(
  null,
  mdtp
)(withRouter(DashboardImportOverlay))
