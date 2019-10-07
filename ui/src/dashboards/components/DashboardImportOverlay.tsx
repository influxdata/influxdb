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

interface OwnProps {
  onDismiss: () => void
}

type Props = OwnProps & DispatchProps & WithRouterProps

class DashboardImportOverlay extends PureComponent<Props> {
  public render() {
    const {onDismiss} = this.props

    return (
      <ImportOverlay
        isVisible={true}
        onDismissOverlay={onDismiss}
        resourceName="Dashboard"
        onSubmit={this.handleImportDashboard}
      />
    )
  }

  private handleImportDashboard = async (
    uploadContent: string
  ): Promise<void> => {
    const {onDismiss} = this.props
    const {createDashboardFromTemplate, populateDashboards} = this.props
    const template = JSON.parse(uploadContent)

    if (_.isEmpty(template)) {
      onDismiss()
    }

    await createDashboardFromTemplate(template)

    await populateDashboards()

    onDismiss()
  }
}

const mdtp: DispatchProps = {
  createDashboardFromTemplate: createDashboardFromTemplateAction,
  populateDashboards: getDashboardsAsync,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(withRouter<OwnProps>(DashboardImportOverlay))
