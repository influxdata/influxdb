import React, {PureComponent} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {CommunityTemplateInstallerOverlay} from 'src/templates/components/CommunityTemplateInstallerOverlay'

// Actions
import {setCommunityTemplateToInstall} from 'src/templates/actions/creators'
import {createTemplate, fetchAndSetStacks} from 'src/templates/actions/thunks'
import {notify} from 'src/shared/actions/notifications'

import {getTotalResourceCount} from 'src/templates/selectors'

// Types
import {AppState, Organization, ResourceType} from 'src/types'
import {ComponentStatus} from '@influxdata/clockface'

// Utils
import {getByID} from 'src/resources/selectors'
import {getGithubUrlFromTemplateDetails} from 'src/templates/utils'

import {installTemplate, reviewTemplate} from 'src/templates/api'

import {communityTemplateInstallSucceeded} from 'src/shared/copy/notifications'

interface State {
  status: ComponentStatus
}

type ReduxProps = ConnectedProps<typeof connector>
type RouterProps = RouteComponentProps<{
  directory: string
  orgID: string
  templateName: string
  templateExtension: string
}>
type Props = ReduxProps & RouterProps

class UnconnectedTemplateImportOverlay extends PureComponent<Props> {
  public state: State = {
    status: ComponentStatus.Default,
  }

  public componentDidMount() {
    const {directory, org, templateExtension, templateName} = this.props
    this.reviewTemplateResources(
      org.id,
      directory,
      templateName,
      templateExtension
    )
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

  private reviewTemplateResources = async (
    orgID,
    directory,
    templateName,
    templateExtension
  ) => {
    const yamlLocation = getGithubUrlFromTemplateDetails(
      directory,
      templateName,
      templateExtension
    )

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
    const {directory, org, templateExtension, templateName} = this.props

    const yamlLocation = getGithubUrlFromTemplateDetails(
      directory,
      templateName,
      templateExtension
    )

    try {
      const summary = await installTemplate(
        org.id,
        yamlLocation,
        this.props.resourcesToSkip
      )

      this.props.notify(communityTemplateInstallSucceeded(templateName))

      this.props.fetchAndSetStacks(org.id)

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
    directory: props.match.params.directory,
    templateName: props.match.params.templateName,
    templateExtension: props.match.params.templateExtension,
    flags: state.flags.original,
    resourceCount: getTotalResourceCount(
      state.resources.templates.communityTemplateToInstall.summary
    ),
    resourcesToSkip:
      state.resources.templates.communityTemplateToInstall.resourcesToSkip,
  }
}

const mdtp = {
  createTemplate,
  notify,
  setCommunityTemplateToInstall,
  fetchAndSetStacks,
}

const connector = connect(mstp, mdtp)

export const CommunityTemplateImportOverlay = connector(
  withRouter(UnconnectedTemplateImportOverlay)
)
