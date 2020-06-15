import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {TemplateInstallerOverlay} from 'src/templates/components/TemplateInstallerOverlay'

// Actions
import {createTemplate as createTemplateAction} from 'src/templates/actions/thunks'
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Types
import {AppState, Organization, ResourceType} from 'src/types'
import {ComponentStatus} from '@influxdata/clockface'

// Utils
import {getByID} from 'src/resources/selectors'

interface State {
  status: ComponentStatus
}

interface DispatchProps {
  createTemplate: typeof createTemplateAction
  notify: typeof notifyAction
}

interface StateProps {
  org: Organization
  templateName: string
}

interface OwnProps extends WithRouterProps {
  params: {orgID: string; templateName: string}
}

type Props = DispatchProps & OwnProps & StateProps

class UnconnectedTemplateImportOverlay extends PureComponent<Props> {
  public state: State = {
    status: ComponentStatus.Default,
  }

  public render() {
    return (
      <TemplateInstallerOverlay
        onDismissOverlay={this.onDismiss}
        onSubmit={this.handleInstallTemplate}
        status={this.state.status}
        templateName={this.props.templateName}
        updateStatus={this.updateOverlayStatus}
      />
    )
  }

  private onDismiss = () => {
    const {router} = this.props

    router.goBack()
  }

  private updateOverlayStatus = (status: ComponentStatus) =>
    this.setState(() => ({status}))

  private handleInstallTemplate = (importString: string) => {
    importString
  }
}

const mstp = (state: AppState, props: Props): StateProps => {
  const org = getByID<Organization>(
    state,
    ResourceType.Orgs,
    props.params.orgID
  )

  return {org, templateName: props.params.templateName}
}

const mdtp: DispatchProps = {
  createTemplate: createTemplateAction,
  notify: notifyAction,
}

export const CommunityTemplateImportOverlay = connect<
  StateProps,
  DispatchProps,
  Props
>(
  mstp,
  mdtp
)(withRouter(UnconnectedTemplateImportOverlay))
