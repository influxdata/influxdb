// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'
import {connect} from 'react-redux'

// Copy
import {invalidJSON} from 'src/shared/copy/notifications'

// Actions
import {
  getDashboardsAsync,
  createDashboardFromTemplate as createDashboardFromTemplateAction,
} from 'src/dashboards/actions'
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Types
import ImportOverlay from 'src/shared/components/ImportOverlay'

interface DispatchProps {
  createDashboardFromTemplate: typeof createDashboardFromTemplateAction
  notify: typeof notifyAction
  populateDashboards: typeof getDashboardsAsync
}

interface OwnProps extends WithRouterProps {
  params: {orgID: string}
}

type Props = OwnProps & DispatchProps

class DashboardImportOverlay extends PureComponent<Props> {
  public render() {
    return (
      <ImportOverlay
        isVisible={true}
        onDismissOverlay={this.onDismiss}
        resourceName="Dashboard"
        onSubmit={this.handleImportDashboard}
      />
    )
  }

  private handleImportDashboard = (uploadContent: string) => {
    const {createDashboardFromTemplate, notify, populateDashboards} = this.props

    let template
    try {
      template = JSON.parse(uploadContent)
    } catch (error) {
      notify(invalidJSON(error.message))
      return
    }

    if (_.isEmpty(template)) {
      this.onDismiss()
    }

    createDashboardFromTemplate(template)
    populateDashboards()
    this.onDismiss()
  }

  private onDismiss = (): void => {
    const {router} = this.props
    router.goBack()
  }
}

const mdtp: DispatchProps = {
  createDashboardFromTemplate: createDashboardFromTemplateAction,
  notify: notifyAction,
  populateDashboards: getDashboardsAsync,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(withRouter(DashboardImportOverlay))
