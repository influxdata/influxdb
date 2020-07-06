import React, {PureComponent} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect} from 'react-redux'

// Components
import {CommunityTemplateInstallerOverlay} from 'src/templates/components/CommunityTemplateInstallerOverlay'

// Actions
import {createTemplate as createTemplateAction} from 'src/templates/actions/thunks'
import {notify as notifyAction} from 'src/shared/actions/notifications'

import {getTotalResourceCount} from 'src/templates/selectors'

import {FlagMap} from 'src/shared/reducers/flags'

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
  flags: FlagMap
  org: Organization
  templateName: string
  resourceCount: number
}

type Props = DispatchProps &
  RouteComponentProps<{orgID: string; templateName: string}> &
  StateProps

class UnconnectedTemplateImportOverlay extends PureComponent<Props> {
  public state: State = {
    status: ComponentStatus.Default,
  }

  public render() {
    if (!this.props.flags.communityTemplates) {
      return null
    }

    return (
      <CommunityTemplateInstallerOverlay
        onDismissOverlay={this.onDismiss}
        onSubmit={this.handleInstallTemplate}
        resourceCount={this.props.resourceCount}
        status={this.state.status}
        templateName={this.props.templateName}
        updateStatus={this.updateOverlayStatus}
      />
    )
  }

  private onDismiss = () => {
    const {history} = this.props

    history.goBack()
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
    props.match.params.orgID
  )

  return {
    org,
    templateName: props.match.params.templateName,
    flags: state.flags.original,
    resourceCount: getTotalResourceCount(
      state.resources.templates.communityTemplateToInstall.summary
    ),
  }
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
