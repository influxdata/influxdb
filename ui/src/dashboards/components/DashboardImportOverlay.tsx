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
import {AppState, Organization} from 'src/types'

interface DispatchProps {
  createDashboardFromTemplate: typeof createDashboardFromTemplateAction
  populateDashboards: typeof getDashboardsAsync
}

interface StateProps {
  orgs: Organization[]
}

interface OwnProps extends WithRouterProps {
  params: {orgID: string}
}

type Props = OwnProps & StateProps & DispatchProps

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
    const {
      createDashboardFromTemplate,
      populateDashboards,
      params: {orgID},
      orgs,
    } = this.props
    const template = JSON.parse(uploadContent)

    if (_.isEmpty(template)) {
      this.onDismiss()
    }

    await createDashboardFromTemplate(template, orgID || orgs[0].id)

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

const mstp = (state: AppState): StateProps => {
  const {orgs} = state
  return {orgs}
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter(DashboardImportOverlay))
