// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'
import {connect} from 'react-redux'

// Actions
import {getDashboardsAsync} from 'src/dashboards/actions'
import {createDashboardFromTemplate as createDashboardFromTemplateAction} from 'src/dashboards/actions'

// Types
import ImportOverlay from 'src/shared/components/ImportOverlay'

interface DispatchProps {
  createDashboardFromTemplate: typeof createDashboardFromTemplateAction
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

  private handleImportDashboard = async (
    uploadContent: string
  ): Promise<void> => {
    const {createDashboardFromTemplate, populateDashboards} = this.props
    const template = JSON.parse(uploadContent)

    if (_.isEmpty(template)) {
      this.onDismiss()
    }

    await createDashboardFromTemplate(template)

    await populateDashboards()

    this.onDismiss()
  }

  private onDismiss = (): void => {
    const {router} = this.props
    router.goBack()
  }
}

const mdtp: DispatchProps = {
  createDashboardFromTemplate: createDashboardFromTemplateAction,
  populateDashboards: getDashboardsAsync,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(withRouter(DashboardImportOverlay))
