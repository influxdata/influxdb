import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {
  Appearance,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
  ConfirmationButton,
  IconFont,
  Table,
  LinkButton,
  VerticalAlignment,
  ButtonShape,
  Alignment,
} from '@influxdata/clockface'
import {CommunityTemplatesResourceSummary} from 'src/templates/components/CommunityTemplatesResourceSummary'

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

import {event} from 'src/cloud/utils/reporting'

interface OwnProps {
  orgID: string
}

type ReduxProps = ConnectedProps<typeof connector>

type Props = OwnProps & ReduxProps

export interface Resource {
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

  private renderStackSources(sources: string[]) {
    return sources.map(source => {
      if (source.includes('github') && source.includes('influxdata')) {
        return (
          <LinkButton
            key={source}
            text="Community Templates"
            icon={IconFont.GitHub}
            href={source}
            size={ComponentSize.Small}
            style={{display: 'inline-block'}}
            target="_blank"
          />
        )
      }

      return source
    })
  }

  private generateDeleteHandlerForStack = (
    stackID: string,
    stackName: string
  ) => {
    return async () => {
      try {
        await deleteStack(stackID, this.props.orgID)

        event('template_delete', {templateName: stackName})

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
              <Table.HeaderCell style={{width: '250px'}}>
                Template Name
              </Table.HeaderCell>
              <Table.HeaderCell style={{width: 'calc(100% - 700px)'}}>
                Installed Resources
              </Table.HeaderCell>
              <Table.HeaderCell style={{width: '180px'}}>
                Install Date
              </Table.HeaderCell>
              <Table.HeaderCell style={{width: '210px'}}>
                Source
              </Table.HeaderCell>
              <Table.HeaderCell style={{width: '60px'}}>
                &nbsp;
              </Table.HeaderCell>
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {this.props.stacks.map(stack => {
              return (
                <Table.Row
                  testID="installed-template-list"
                  key={`stack-${stack.id}`}
                >
                  <Table.Cell
                    testID={`installed-template-${stack.name}`}
                    verticalAlignment={VerticalAlignment.Top}
                  >
                    <span className="community-templates--resources-table-item">
                      {stack.name}
                    </span>
                  </Table.Cell>
                  <Table.Cell
                    testID="template-resource-link"
                    verticalAlignment={VerticalAlignment.Top}
                  >
                    <CommunityTemplatesResourceSummary
                      resources={stack.resources}
                      stackID={stack.id}
                      orgID={this.props.orgID}
                    />
                  </Table.Cell>
                  <Table.Cell verticalAlignment={VerticalAlignment.Top}>
                    <span className="community-templates--resources-table-item">
                      {new Date(stack.createdAt).toDateString()}
                    </span>
                  </Table.Cell>
                  <Table.Cell
                    testID="template-source-link"
                    verticalAlignment={VerticalAlignment.Top}
                  >
                    {this.renderStackSources(stack.sources)}
                  </Table.Cell>
                  <Table.Cell
                    verticalAlignment={VerticalAlignment.Top}
                    horizontalAlignment={Alignment.Right}
                  >
                    <ConfirmationButton
                      confirmationButtonText="Delete"
                      testID={`template-delete-button-${stack.name}`}
                      confirmationButtonColor={ComponentColor.Danger}
                      confirmationLabel="Really Delete All Resources?"
                      popoverColor={ComponentColor.Default}
                      popoverAppearance={Appearance.Solid}
                      onConfirm={this.generateDeleteHandlerForStack(
                        stack.id,
                        stack.name
                      )}
                      icon={IconFont.Trash}
                      color={ComponentColor.Danger}
                      size={ComponentSize.Small}
                      status={ComponentStatus.Default}
                      shape={ButtonShape.Square}
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
