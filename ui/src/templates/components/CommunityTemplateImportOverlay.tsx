import React, {PureComponent} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {CommunityTemplateInstallerOverlay} from 'src/templates/components/CommunityTemplateInstallerOverlay'

// Actions
import {setCommunityTemplateToInstall} from 'src/templates/actions/creators'
import {createTemplate as createTemplateAction} from 'src/templates/actions/thunks'
import {notify as notifyAction} from 'src/shared/actions/notifications'

import {getTotalResourceCount} from 'src/templates/selectors'

// Types
import {AppState, Organization, ResourceType} from 'src/types'
import {ComponentStatus} from '@influxdata/clockface'

// Utils
import {getByID} from 'src/resources/selectors'
import {
  getGithubUrlFromTemplateName,
  getRawUrlFromGithub,
} from 'src/templates/utils'

import {installTemplate, reviewTemplate} from 'src/templates/api'

import {communityTemplateInstallSucceeded} from 'src/shared/copy/notifications'

interface State {
  status: ComponentStatus
}

type ReduxProps = ConnectedProps<typeof connector>
type RouterProps = RouteComponentProps<{orgID: string; templateName: string}>
type Props = ReduxProps & RouterProps

class UnconnectedTemplateImportOverlay extends PureComponent<Props> {
  public state: State = {
    status: ComponentStatus.Default,
  }

  public componentDidMount() {
    const {org, templateName} = this.props

    this.reviewTemplateResources(org.id, templateName)
  }

  public render() {
    if (!this.props.flags.communityTemplates) {
      return null
    }

    return (
      <CommunityTemplateInstallerOverlay
        onDismissOverlay={this.onDismiss}
        onInstall={this.handleInstallTemplate}
        resourceCount={this.props.resourceCount}
        status={this.state.status}
        templateName={this.props.templateName}
        updateStatus={this.updateOverlayStatus}
      />
    )
  }

  private reviewTemplateResources = async (orgID, templateName) => {
    const yamlLocation = `${getRawUrlFromGithub(
      getGithubUrlFromTemplateName(templateName)
    )}/${templateName}.yml`

    try {
      const summary = await reviewTemplate(orgID, yamlLocation)

      this.props.setCommunityTemplateToInstall(summary)
      return summary
    } catch (err) {
      console.error(err)
    }
  }

  private onDismiss = () => {
    const {history} = this.props

    history.goBack()
  }

  private updateOverlayStatus = (status: ComponentStatus) =>
    this.setState(() => ({status}))

  private handleInstallTemplate = async () => {
    const {org, templateName} = this.props

    const yamlLocation = `${getRawUrlFromGithub(
      getGithubUrlFromTemplateName(templateName)
    )}/${templateName}.yml`

    try {
      const summary = await installTemplate(org.id, yamlLocation)
      this.props.notify(communityTemplateInstallSucceeded(templateName))

      this.onDismiss()

      return summary
    } catch (err) {
      console.error('Error installing template', err)
    }
  }
}

const mstp = (state: AppState, props: RouterProps) => {
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

const mdtp = {
  createTemplate: createTemplateAction,
  notify: notifyAction,
  setCommunityTemplateToInstall,
}

const connector = connect(mstp, mdtp)

export const CommunityTemplateImportOverlay = connector(
  withRouter(UnconnectedTemplateImportOverlay)
)
