import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {Link} from 'react-router-dom'

// Components
import {
  Appearance,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
  ConfirmationButton,
  IconFont,
  Table,
} from '@influxdata/clockface'

// Redux
import {notify} from 'src/shared/actions/notifications'
import {
  communityTemplateDeleteSucceeded,
  communityTemplateDeleteFailed,
  communityTemplateFetchStackFailed,
} from 'src/shared/copy/notifications'
import {fetchAndSetStacks} from 'src/templates/actions/thunks'

// Types
import {AppState} from 'src/types'
import {TemplateKind} from 'src/client'

// API
import {deleteStack} from 'src/templates/api'

//Utils
import {reportError} from 'src/shared/utils/errors'

interface OwnProps {
  orgID: string
}

type ReduxProps = ConnectedProps<typeof connector>

type Props = OwnProps & ReduxProps

interface Resource {
  apiVersion?: string
  resourceID?: string
  kind?: TemplateKind
  templateMetaName?: string
  associations?: {
    kind?: TemplateKind
    metaName?: string
  }[]
}

class CommunityTemplatesInstalledListUnconnected extends PureComponent<Props> {
  public componentDidMount() {
    try {
      this.props.fetchAndSetStacks(this.props.orgID)
    } catch (err) {
      this.props.notify(communityTemplateFetchStackFailed(err.message))
      reportError(err, {name: 'The community template fetch stack failed'})
    }
  }

  private renderStackResources(resources: Resource[]) {
    return resources.map(resource => {
      switch (resource.kind) {
        case 'Bucket': {
          return (
            <React.Fragment key={resource.templateMetaName}>
              <Link to={`/orgs/${this.props.orgID}/load-data/buckets`}>
                {resource.kind} <code>{resource.templateMetaName}</code>
              </Link>
              <br />
            </React.Fragment>
          )
        }
        case 'Check':
        case 'CheckDeadman':
        case 'CheckThreshold': {
          return (
            <React.Fragment key={resource.templateMetaName}>
              <Link
                to={`/orgs/${this.props.orgID}/alerting/checks/${resource.resourceID}/edit`}
              >
                {resource.kind} <code>{resource.templateMetaName}</code>
              </Link>
              <br />
            </React.Fragment>
          )
        }
        case 'Dashboard': {
          return (
            <React.Fragment key={resource.templateMetaName}>
              <Link
                to={`/orgs/${this.props.orgID}/dashboards/${resource.resourceID}`}
              >
                {resource.kind} <code>{resource.templateMetaName}</code>
              </Link>
              <br />
            </React.Fragment>
          )
        }
        case 'Label': {
          return (
            <React.Fragment key={resource.templateMetaName}>
              <Link to={`/orgs/${this.props.orgID}/settings/labels`}>
                {resource.kind} <code>{resource.templateMetaName}</code>
              </Link>
              <br />
            </React.Fragment>
          )
        }
        case 'NotificationEndpoint':
        case 'NotificationEndpointHTTP':
        case 'NotificationEndpointPagerDuty':
        case 'NotificationEndpointSlack': {
          return (
            <React.Fragment key={resource.templateMetaName}>
              <Link
                to={`/orgs/${this.props.orgID}/alerting/endpoints/${resource.resourceID}/edit`}
              >
                {resource.kind} <code>{resource.templateMetaName}</code>
              </Link>
              <br />
            </React.Fragment>
          )
        }
        case 'NotificationRule': {
          return (
            <React.Fragment key={resource.templateMetaName}>
              <Link
                to={`/orgs/${this.props.orgID}/alerting/rules/${resource.resourceID}/edit`}
              >
                {resource.kind} <code>{resource.templateMetaName}</code>
              </Link>
              <br />
            </React.Fragment>
          )
        }
        case 'Task': {
          return (
            <React.Fragment key={resource.templateMetaName}>
              <Link
                to={`/orgs/${this.props.orgID}/tasks/${resource.resourceID}/edit`}
              >
                {resource.kind} <code>{resource.templateMetaName}</code>
              </Link>
              <br />
            </React.Fragment>
          )
        }
        case 'Telegraf': {
          return (
            <React.Fragment key={resource.templateMetaName}>
              <Link
                to={`/orgs/${this.props.orgID}/load-data/telegrafs/${resource.resourceID}/view`}
              >
                {resource.kind} <code>{resource.templateMetaName}</code>
              </Link>
              <br />
            </React.Fragment>
          )
        }
        case 'Variable': {
          return (
            <React.Fragment key={resource.templateMetaName}>
              <Link
                to={`/orgs/${this.props.orgID}/settings/variables/${resource.resourceID}/edit`}
              >
                {resource.kind} <code>{resource.templateMetaName}</code>
              </Link>
              <br />
            </React.Fragment>
          )
        }
        default: {
          return (
            <React.Fragment key={resource.templateMetaName}>
              {resource.kind}
              <br />
            </React.Fragment>
          )
        }
      }
    })
  }

  private renderStackSources(sources: string[]) {
    return sources.map(source => {
      if (source.includes('github')) {
        return (
          <a key={source} href={source}>
            {source}
          </a>
        )
      }

      return source
    })
  }

  private generateDeleteHandlerForStack = (stackID: string, stackName: string) => {
    return async () => {
      try {
        await deleteStack(stackID, this.props.orgID)

        this.props.notify(communityTemplateDeleteSucceeded(stackName))
      } catch (err) {
        this.props.notify(communityTemplateDeleteFailed(err.message))
        reportError(err, {name: 'The community template delete failed'})
      } finally {
        this.props.fetchAndSetStacks(this.props.orgID)
      }
    }
  }

  render() {
    if (!this.props.stacks.length) {
      return <h4>You haven't installed any templates yet</h4>
    }

    return (
      <>
        <h2>Installed Templates</h2>
        <Table striped={true} highlight={true}>
          <Table.Header>
            <Table.Row>
              <Table.HeaderCell>Template Name</Table.HeaderCell>
              <Table.HeaderCell>Resources Created</Table.HeaderCell>
              <Table.HeaderCell>Install Date</Table.HeaderCell>
              <Table.HeaderCell>Source</Table.HeaderCell>
              <Table.HeaderCell>&nbsp;</Table.HeaderCell>
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {this.props.stacks.map(stack => {
              return (
                <Table.Row key={`stack-${stack.id}`}>
                  <Table.Cell>{stack.name}</Table.Cell>
                  <Table.Cell>
                    {this.renderStackResources(stack.resources)}
                  </Table.Cell>
                  <Table.Cell>
                    {new Date(stack.createdAt).toDateString()}
                  </Table.Cell>
                  <Table.Cell>
                    {this.renderStackSources(stack.sources)}
                  </Table.Cell>
                  <Table.Cell>
                    <ConfirmationButton
                      confirmationButtonText="Delete"
                      confirmationButtonColor={ComponentColor.Danger}
                      confirmationLabel="Really Delete All Resources?"
                      popoverColor={ComponentColor.Default}
                      popoverAppearance={Appearance.Solid}
                      onConfirm={this.generateDeleteHandlerForStack(stack.id, stack.name)}
                      icon={IconFont.Trash}
                      color={ComponentColor.Danger}
                      size={ComponentSize.Small}
                      status={ComponentStatus.Default}
                    />
                  </Table.Cell>
                </Table.Row>
              )
            })}
          </Table.Body>
        </Table>
      </>
    )
  }
}

const mstp = (state: AppState) => {
  return {
    stacks: state.resources.templates.stacks.filter(
      stack => stack.eventType !== 'delete' && stack.eventType !== 'uninstall'
    ),
  }
}

const mdtp = {
  fetchAndSetStacks,
  notify,
}

const connector = connect(mstp, mdtp)

export const CommunityTemplatesInstalledList = connector(
  CommunityTemplatesInstalledListUnconnected
)
