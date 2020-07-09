import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {Table} from '@influxdata/clockface'

// Redux
import {fetchAndSetStacks} from 'src/templates/actions/thunks'

// Types
import {AppState} from 'src/types'
import {TemplateKind} from 'src/client'

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

class CommunityTemplatesActivityLogUnconnected extends PureComponent<Props> {
  public componentDidMount() {
    try {
      this.props.fetchAndSetStacks(this.props.orgID)
    } catch (err) {
      console.error('error getting stacks', err)
    }
  }

  private renderStackResources(resources: Resource[]) {
    return resources.map(resource => {
      return (
        <React.Fragment key={resource.templateMetaName}>
          {resource.kind}
          <br />
        </React.Fragment>
      )
    })
  }

  private renderStackSources(sources: string[]) {
    return sources.map(source => {
      if (source.includes('github')) {
        return (
          <a key={source} href={source}>
            Github
          </a>
        )
      }

      return source
    })
  }

  render() {
    if (!this.props.stacks.length) {
      return <h4>You haven't installed any templates yet</h4>
    }

    return (
      <>
        <h2>Activity Log</h2>
        <Table striped={true} highlight={true}>
          <Table.Header>
            <Table.Row>
              <Table.HeaderCell>Template Name</Table.HeaderCell>
              <Table.HeaderCell>Resources Created</Table.HeaderCell>
              <Table.HeaderCell>Install Date</Table.HeaderCell>
              <Table.HeaderCell>Source</Table.HeaderCell>
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
      stack => stack.eventType !== 'delete'
    ),
  }
}

const mdtp = {
  fetchAndSetStacks,
}

const connector = connect(mstp, mdtp)

export const CommunityTemplatesActivityLog = connector(
  CommunityTemplatesActivityLogUnconnected
)
